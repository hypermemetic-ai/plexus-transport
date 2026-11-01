//! RPC-6 under tokio-tungstenite: a message over the cap is refused and the
//! socket kept. tungstenite enforces its limits by failing the read, and
//! tokio-tungstenite's stream ends at its first error — and a frame refused
//! at its header leaves its payload unread, the stream out of step. So the
//! cap is kept below it, here: [`Guard`] reads the client's bytes before
//! tungstenite does, follows each frame's header, and
//!
//! - hands a frame within the cap on as it came (control frames always);
//! - holds a fragmented message's frames until its last one, so a message
//!   that grows past the cap is dropped whole, never half-seen;
//! - drops a message past the cap — its frames, as they stream in, without
//!   keeping them — and counts it in [`Dropped`], which the socket's loop
//!   answers `-32007` (`id: null`, spec/01).
//!
//! Everything else — masking, opcodes, UTF-8, control frames, a stray
//! continuation (passed on, refused there) — stays tungstenite's. Writes
//! pass straight through.

use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// The messages a socket's guard dropped over the cap: its loop answers each
/// `-32007` after every read (the guard runs inside that read).
#[derive(Debug, Default)]
pub(crate) struct Dropped {
    count: AtomicU64,
}

impl Dropped {
    pub(crate) fn count(&self) -> u64 {
        self.count.load(Ordering::Acquire)
    }

    fn one(&self) {
        self.count.fetch_add(1, Ordering::AcqRel);
    }
}

/// Where the guard is in the client's byte stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum In {
    /// At a frame's header.
    Header,
    /// Handing this many bytes on as they come (a header still in `buf`, and
    /// its payload).
    Pass(u64),
    /// Keeping this many payload bytes of a fragment in `hold`.
    Hold(u64),
    /// Dropping this many payload bytes.
    Skip(u64),
}

/// Where the current data message stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Msg {
    /// Between messages, or inside one passed on whole (unfragmented).
    None,
    /// A fragmented message within the cap so far: its frames in `hold`,
    /// released at its last.
    Holding { len: u64 },
    /// A message over the cap: its frames dropped until its last.
    Skipping,
}

/// A frame header as the guard reads it.
#[derive(Debug, Clone, Copy)]
struct Header {
    fin: bool,
    opcode: u8,
    len: u64,
    size: usize,
}

impl Header {
    /// The frame's bytes, header and payload. The length is the client's own
    /// 64 bits: saturated, so a huge frame stays huge rather than wrapping
    /// short and leaving its payload to be read as headers. Nothing sends
    /// 2^64 bytes, and tungstenite refuses the frame first.
    fn total(&self) -> u64 {
        (self.size as u64).saturating_add(self.len)
    }
}

/// `Some` once the whole header is in `b`.
fn header(b: &[u8]) -> Option<Header> {
    if b.len() < 2 {
        return None;
    }
    let fin = b[0] & 0x80 != 0;
    let opcode = b[0] & 0x0f;
    let masked = b[1] & 0x80 != 0;
    let (len, mut size) = match b[1] & 0x7f {
        126 => (u16::from_be_bytes([*b.get(2)?, *b.get(3)?]) as u64, 4),
        127 => {
            let e: [u8; 8] = b.get(2..10)?.try_into().ok()?;
            (u64::from_be_bytes(e), 10)
        }
        n => (n as u64, 2),
    };
    if masked {
        size += 4;
    }
    (b.len() >= size).then_some(Header { fin, opcode, len, size })
}

/// The read side of a socket, a frame's length ahead of tungstenite (see
/// the module docs).
pub(crate) struct Guard<S> {
    io: S,
    cap: u64,
    /// Read from the socket, not yet handed on.
    buf: BytesMut,
    /// Released fragments, handed on before anything in `buf`.
    ready: BytesMut,
    /// The fragments of the message being held.
    hold: BytesMut,
    state: In,
    msg: Msg,
    /// The fragment being held is its message's last: release at its end.
    release: bool,
    dropped: Arc<Dropped>,
}

impl<S> Guard<S> {
    pub(crate) fn new(io: S, cap: usize) -> Self {
        Self {
            io,
            cap: cap as u64,
            buf: BytesMut::with_capacity(8 * 1024),
            ready: BytesMut::new(),
            hold: BytesMut::new(),
            state: In::Header,
            msg: Msg::None,
            release: false,
            dropped: Arc::new(Dropped::default()),
        }
    }

    pub(crate) fn dropped(&self) -> Arc<Dropped> {
        self.dropped.clone()
    }

    /// A whole header is at the front of `buf`: what becomes of its frame.
    fn decide(&mut self, h: Header) {
        let control = h.opcode & 0x08 != 0;
        if control {
            // Ping, pong, close — and anything malformed, tungstenite's to refuse.
            self.state = In::Pass(h.total());
            return;
        }
        let over = |len: u64| len > self.cap;
        match (h.opcode, self.msg) {
            // A continuation of a message being dropped: dropped.
            (0, Msg::Skipping) => {
                self.buf.advance(h.size);
                self.state = In::Skip(h.len);
                if h.fin {
                    self.msg = Msg::None;
                }
            }
            // A continuation of a held message: held, or the whole message
            // dropped once it is past the cap.
            (0, Msg::Holding { len }) => {
                // Both lengths are the client's: a total that overflows is
                // past any cap.
                let total = len.checked_add(h.len).filter(|&t| !over(t));
                let Some(total) = total else {
                    self.buf.advance(h.size);
                    self.hold.clear();
                    self.state = In::Skip(h.len);
                    self.msg = if h.fin { Msg::None } else { Msg::Skipping };
                    self.dropped.one();
                    return;
                };
                self.msg = Msg::Holding { len: total };
                self.hold_frame(h);
            }
            // A new message while one is held: a protocol error. Release
            // what is held, so tungstenite sees the frames as sent and
            // refuses them.
            (_, Msg::Holding { .. }) => {
                let held = self.hold.split();
                self.ready.unsplit(held);
                self.msg = Msg::None;
                self.state = In::Pass(h.total());
            }
            // A stray continuation, or a new message while one is being
            // dropped: passed on as it came (tungstenite refuses the first).
            (0, Msg::None) | (_, Msg::Skipping) => {
                self.msg = Msg::None;
                self.state = In::Pass(h.total());
            }
            // A new message.
            (_, Msg::None) => {
                if over(h.len) {
                    self.buf.advance(h.size);
                    self.state = In::Skip(h.len);
                    if !h.fin {
                        self.msg = Msg::Skipping;
                    }
                    self.dropped.one();
                } else if h.fin {
                    self.state = In::Pass(h.total());
                } else {
                    self.hold.clear();
                    self.msg = Msg::Holding { len: h.len };
                    self.hold_frame(h);
                }
            }
        }
    }

    /// The frame whose header is at the front of `buf` is held: its header
    /// bytes as they came (the mask key with them), then its payload.
    fn hold_frame(&mut self, h: Header) {
        self.hold.extend_from_slice(&self.buf[..h.size]);
        self.buf.advance(h.size);
        self.release = h.fin;
        self.state = In::Hold(h.len);
        if h.len == 0 {
            self.end_hold();
        }
    }

    /// The fragment being held is complete: the next header, and the whole
    /// message released if this was its last.
    fn end_hold(&mut self) {
        self.state = In::Header;
        if self.release {
            let held = self.hold.split();
            self.ready.unsplit(held);
            self.msg = Msg::None;
            self.release = false;
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for Guard<S> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, out: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let this = &mut *self;
        loop {
            if !this.ready.is_empty() {
                let k = this.ready.len().min(out.remaining());
                out.put_slice(&this.ready[..k]);
                this.ready.advance(k);
                return Poll::Ready(Ok(()));
            }
            match this.state {
                In::Pass(0) | In::Skip(0) => {
                    this.state = In::Header;
                    continue;
                }
                In::Hold(0) => {
                    this.end_hold();
                    continue;
                }
                In::Pass(n) => {
                    if !this.buf.is_empty() {
                        let k = (n.min(this.buf.len() as u64) as usize).min(out.remaining());
                        out.put_slice(&this.buf[..k]);
                        this.buf.advance(k);
                        this.state = In::Pass(n - k as u64);
                        return Poll::Ready(Ok(()));
                    }
                    // Nothing buffered: straight from the socket into the
                    // reader's buffer, at most the rest of this frame.
                    if out.remaining() == 0 {
                        return Poll::Ready(Ok(()));
                    }
                    let want = (n.min(out.remaining() as u64)) as usize;
                    let mut sub = out.take(want);
                    ready!(Pin::new(&mut this.io).poll_read(cx, &mut sub))?;
                    let k = sub.filled().len();
                    // SAFETY: `sub` borrowed `out`'s unfilled part; `k` bytes of it are now filled.
                    unsafe { out.assume_init(k) };
                    out.advance(k);
                    if k > 0 {
                        this.state = In::Pass(n - k as u64);
                    }
                    return Poll::Ready(Ok(()));
                }
                In::Hold(n) => {
                    if !this.buf.is_empty() {
                        let k = n.min(this.buf.len() as u64) as usize;
                        this.hold.extend_from_slice(&this.buf[..k]);
                        this.buf.advance(k);
                        this.state = In::Hold(n - k as u64);
                        if n == k as u64 {
                            this.end_hold();
                        }
                        continue;
                    }
                }
                In::Skip(n) => {
                    if !this.buf.is_empty() {
                        let k = n.min(this.buf.len() as u64) as usize;
                        this.buf.advance(k);
                        this.state = In::Skip(n - k as u64);
                        continue;
                    }
                }
                In::Header => {
                    if let Some(h) = header(&this.buf) {
                        this.decide(h);
                        continue;
                    }
                }
            }
            // More of the client's bytes are needed.
            if this.buf.capacity() - this.buf.len() < 4096 {
                this.buf.reserve(8 * 1024);
            }
            let n = ready!(tokio_util_poll_read_buf(Pin::new(&mut this.io), cx, &mut this.buf))?;
            if n == 0 {
                // The end of the stream: whatever was mid-frame is lost, and
                // tungstenite reads the end.
                return Poll::Ready(Ok(()));
            }
        }
    }
}

/// Read into `buf`'s spare capacity (what tokio-util's `poll_read_buf` does).
fn tokio_util_poll_read_buf<S: AsyncRead>(io: Pin<&mut S>, cx: &mut Context<'_>, buf: &mut BytesMut) -> Poll<io::Result<usize>> {
    use bytes::BufMut;
    let dst = buf.chunk_mut();
    // SAFETY: the spare capacity, which ReadBuf treats as uninitialised.
    let dst = unsafe { dst.as_uninit_slice_mut() };
    let mut rb = ReadBuf::uninit(dst);
    ready!(io.poll_read(cx, &mut rb))?;
    let n = rb.filled().len();
    // SAFETY: the read filled `n` bytes of the spare capacity.
    unsafe { buf.advance_mut(n) };
    Poll::Ready(Ok(n))
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Guard<S> {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, data: &[u8]) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, data)
    }

    fn poll_write_vectored(mut self: Pin<&mut Self>, cx: &mut Context<'_>, bufs: &[io::IoSlice<'_>]) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    /// One masked client frame.
    fn frame(op: u8, fin: bool, payload: &[u8]) -> Vec<u8> {
        let mut f = vec![if fin { 0x80 } else { 0 } | op];
        let len = payload.len();
        if len < 126 {
            f.push(0x80 | len as u8);
        } else if len <= u16::MAX as usize {
            f.push(0x80 | 126);
            f.extend_from_slice(&(len as u16).to_be_bytes());
        } else {
            f.push(0x80 | 127);
            f.extend_from_slice(&(len as u64).to_be_bytes());
        }
        let key = [7u8, 9, 11, 13];
        f.extend_from_slice(&key);
        f.extend(payload.iter().enumerate().map(|(i, b)| b ^ key[i % 4]));
        f
    }

    /// Everything the guard hands on, read `chunk` bytes at a time.
    async fn through(input: Vec<u8>, cap: usize, chunk: usize) -> (Vec<u8>, u64) {
        let mut g = Guard::new(std::io::Cursor::new(input), cap);
        let dropped = g.dropped();
        let mut out = Vec::new();
        let mut buf = vec![0u8; chunk];
        loop {
            let n = g.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            out.extend_from_slice(&buf[..n]);
        }
        (out, dropped.count())
    }

    #[tokio::test]
    async fn frames_within_the_cap_pass_as_they_came_whatever_the_reads() {
        let mut input = Vec::new();
        for n in 0..1000u32 {
            input.extend(frame(0x1, true, format!("{{\"n\":{n}}}").as_bytes()));
        }
        input.extend(frame(0x9, true, b"ping"));
        input.extend(frame(0x2, true, &vec![3u8; 70_000]));
        for chunk in [1, 7, 14, 4096, 1 << 20] {
            let (out, dropped) = through(input.clone(), 1 << 20, chunk).await;
            assert_eq!(dropped, 0);
            assert!(out == input, "a {chunk}-byte reader saw the stream changed");
        }
    }

    #[tokio::test]
    async fn a_message_over_the_cap_is_dropped_whole_and_counted() {
        let small = frame(0x1, true, b"{}");
        let mut input = small.clone();
        input.extend(frame(0x1, true, &vec![b'x'; 2000]));
        input.extend(small.clone());
        let (out, dropped) = through(input, 1000, 64).await;
        assert_eq!(dropped, 1);
        assert_eq!(out, [small.clone(), small].concat());
    }

    #[tokio::test]
    async fn fragments_are_held_to_their_last_and_dropped_whole_past_the_cap() {
        let (a, b, c) = (frame(0x1, false, b"{\"x\":"), frame(0x0, false, b"12"), frame(0x0, true, b"3}"));
        let ping = frame(0x9, true, b"p");
        // Within the cap: the ping first (control frames are not held), then the message whole.
        let input = [a.clone(), ping.clone(), b.clone(), c.clone()].concat();
        let (out, dropped) = through(input, 100, 3).await;
        assert_eq!(dropped, 0);
        assert_eq!(out, [ping.clone(), a.clone(), b.clone(), c.clone()].concat());
        // Past it: dropped whole, the next message on.
        let big = frame(0x0, false, &vec![b'y'; 200]);
        let next = frame(0x1, true, b"{}");
        let input = [a, big, ping.clone(), c, next.clone()].concat();
        let (out, dropped) = through(input, 100, 5).await;
        assert_eq!(dropped, 1);
        assert_eq!(out, [ping, next].concat());
    }

    #[tokio::test]
    async fn a_stray_continuation_is_handed_on_for_tungstenite_to_refuse() {
        let stray = frame(0x0, true, b"x");
        let (out, dropped) = through(stray.clone(), 100, 16).await;
        assert_eq!((out, dropped), (stray, 0));
    }

    /// A masked frame's header alone, naming a 64-bit length (no payload).
    fn long_header(op: u8, fin: bool, len: u64) -> Vec<u8> {
        let mut f = vec![if fin { 0x80 } else { 0 } | op, 0x80 | 127];
        f.extend_from_slice(&len.to_be_bytes());
        f.extend_from_slice(&[7, 9, 11, 13]);
        f
    }

    #[tokio::test]
    async fn a_continuation_whose_length_wraps_the_message_total_is_over_the_cap() {
        let first = frame(0x1, false, b"12345");
        let input = [first, long_header(0x0, true, u64::MAX - 4)].concat();
        let (out, dropped) = through(input, 100, 16).await;
        assert!(out.is_empty(), "nothing of the message is handed on");
        assert_eq!(dropped, 1);
    }

    #[tokio::test]
    async fn a_header_and_length_that_wrap_are_passed_on_never_taken_for_a_short_frame() {
        let held = frame(0x1, false, b"12345");
        let cases = [
            ("control", vec![long_header(0x9, true, u64::MAX)]),
            ("stray continuation", vec![long_header(0x0, true, u64::MAX)]),
            ("a new message while one is held", vec![held, long_header(0x1, true, u64::MAX)]),
        ];
        for (name, frames) in cases {
            // The bytes after the header are the frame's payload, however
            // long it claims to be: never read as a header of their own.
            let tail = frame(0x1, true, &vec![b'x'; 2000]);
            let input = [frames.concat(), tail].concat();
            let (out, dropped) = through(input.clone(), 100, 16).await;
            assert_eq!(dropped, 0, "{name}");
            assert!(out == input, "{name}: the stream changed");
        }
    }
}
