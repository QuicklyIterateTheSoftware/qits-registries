package eu.wohlben.qits.registry;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The backpressure half of {@link BlobSender}, driven through a write stream this suite holds full
 * at will.
 *
 * <p>The property under test is the one the old {@code sendFile} got for free and this loop has to
 * earn: <b>a fast source and a stopped reader must not meet in memory</b>. PostgreSQL answers a
 * chunk in microseconds and a client on a slow link takes seconds over it, so a sender that did not
 * park would queue a whole layer in heap for every slow puller — the failure that only shows up in
 * production, under load, on the largest images.
 *
 * <p>No Quarkus, no HTTP and no database here: {@code HttpServerResponse} is a {@code
 * WriteStream<Buffer>}, and a stream is the smallest thing that can express "the client has stopped
 * reading" exactly.
 */
class BlobSenderTest {

  private static final Duration PATIENT = Duration.ofSeconds(30);

  @Test
  @DisplayName("a reader that has stopped stops the sender, and the blob is not read ahead of it")
  void aStoppedReaderStopsTheSender() throws Exception {
    TestSink sink = new TestSink();
    sink.stallAfterEachWrite = true;
    byte[] blob = blobOf(5 * BlobSender.READ_SIZE + 17);
    CountingStream source = new CountingStream(blob);

    Sending sending = Sending.start(sink, source, PATIENT);
    awaitWritten(sink, 1);
    Thread.sleep(100);

    assertEquals(1, sink.written.size(), "the sender must park on the first full write queue");
    assertEquals(
        BlobSender.READ_SIZE,
        source.read,
        "and it must hold exactly one read buffer, not the rest of the blob");

    sending.readEverything(sink);

    assertEquals(blob.length, sending.sent(), "every byte is accounted for");
    assertArrayEquals(blob, sink.bytes(), "and arrives whole, in order");
  }

  @Test
  @DisplayName("a reader that keeps up is never parked")
  void aReaderThatKeepsUpIsNeverParked() throws Exception {
    TestSink sink = new TestSink();
    byte[] blob = blobOf(3 * BlobSender.READ_SIZE);

    Sending sending = Sending.start(sink, new CountingStream(blob), PATIENT);
    sending.finish();

    assertEquals(blob.length, sending.sent());
    assertArrayEquals(blob, sink.bytes());
  }

  @Test
  @DisplayName("a reader that never drains is given up on rather than held forever")
  void aReaderThatNeverDrainsIsGivenUpOn() {
    TestSink sink = new TestSink();
    sink.stallAfterEachWrite = true;

    BlobSender.SendAborted aborted =
        assertThrows(
            BlobSender.SendAborted.class,
            () ->
                BlobSender.pump(
                    sink,
                    new ByteArrayInputStream(blobOf(2 * BlobSender.READ_SIZE)),
                    Duration.ofMillis(50)));

    assertTrue(aborted.getMessage().contains("read nothing"), aborted.getMessage());
  }

  @Test
  @DisplayName("a connection that dies under a parked send ends it at once, not at the timeout")
  void aDeadConnectionEndsTheSendAtOnce() throws Exception {
    TestSink sink = new TestSink();
    sink.stallAfterEachWrite = true;
    sink.holdWrites = true;

    Sending sending =
        Sending.start(sink, new CountingStream(blobOf(2 * BlobSender.READ_SIZE)), PATIENT);
    awaitWritten(sink, 1);
    Thread.sleep(50);
    sink.killConnection();

    // Well inside PATIENT: a send woken by the timeout instead of by the failed write would still
    // be parked here, holding a worker thread for a client that is already gone.
    sending.awaitEnd(Duration.ofSeconds(5));
    assertEquals(
        BlobSender.SendAborted.class,
        sending.thrown().getClass(),
        "a failed write must wake the park it happened under");
  }

  // --- fixtures ---------------------------------------------------------------------------------

  private static byte[] blobOf(int size) {
    byte[] bytes = new byte[size];
    for (int i = 0; i < size; i++) {
      bytes[i] = (byte) (i % 251);
    }
    return bytes;
  }

  private static void awaitWritten(TestSink sink, int count) throws InterruptedException {
    long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
    while (sink.written.size() < count && System.nanoTime() < deadline) {
      Thread.sleep(1);
    }
  }

  /** One {@code pump} running on a thread of its own, so the test can be the client. */
  private static final class Sending {

    private final Thread thread;
    private final AtomicReference<Throwable> thrown = new AtomicReference<>();
    private volatile long sent;

    private Sending(TestSink sink, InputStream source, Duration drainTimeout) {
      this.thread =
          new Thread(
              () -> {
                try {
                  sent = BlobSender.pump(sink, source, drainTimeout);
                } catch (Throwable t) {
                  thrown.set(t);
                }
              },
              "blob-sender-under-test");
    }

    static Sending start(TestSink sink, InputStream source, Duration drainTimeout) {
      Sending sending = new Sending(sink, source, drainTimeout);
      sending.thread.start();
      return sending;
    }

    /** Plays the client: drains until the sender has nothing left to hand over. */
    void readEverything(TestSink sink) throws Exception {
      while (thread.isAlive()) {
        sink.drain();
        Thread.sleep(1);
      }
      finish();
    }

    void finish() throws Exception {
      awaitEnd(Duration.ofSeconds(10));
      if (thrown.get() != null) {
        throw new AssertionError("the send failed", thrown.get());
      }
    }

    void awaitEnd(Duration within) throws InterruptedException {
      thread.join(within.toMillis());
      assertTrue(!thread.isAlive(), "the send did not end within " + within);
    }

    Throwable thrown() {
      return thrown.get();
    }

    long sent() {
      return sent;
    }
  }

  /** A blob source that says how much of itself has been pulled. */
  private static final class CountingStream extends InputStream {

    private final ByteArrayInputStream bytes;
    volatile int read;

    CountingStream(byte[] blob) {
      this.bytes = new ByteArrayInputStream(blob);
    }

    @Override
    public int read() {
      int one = bytes.read();
      if (one != -1) {
        read++;
      }
      return one;
    }

    @Override
    public int read(byte[] buffer, int off, int len) {
      int taken = bytes.read(buffer, off, len);
      if (taken > 0) {
        read += taken;
      }
      return taken;
    }
  }

  /**
   * A write stream that is full exactly when this test says so — the client, expressed as the only
   * thing the sender can see of it.
   */
  private static final class TestSink implements WriteStream<Buffer> {

    final List<Buffer> written = Collections.synchronizedList(new ArrayList<>());

    /** Whether one write is enough to fill the queue. The slow client, in one flag. */
    volatile boolean stallAfterEachWrite;

    /** Whether a write stays unfinished until {@link #killConnection} settles it. */
    volatile boolean holdWrites;

    private final List<Promise<Void>> held = Collections.synchronizedList(new ArrayList<>());
    private volatile boolean full;
    private volatile Handler<Void> drainHandler;

    /** The client reading: makes room, and tells whoever is parked. */
    void drain() {
      full = false;
      Handler<Void> waiting = drainHandler;
      if (waiting != null) {
        waiting.handle(null);
      }
    }

    /** The socket dying: every write still in flight fails, and no drain will ever come. */
    void killConnection() {
      synchronized (held) {
        held.forEach(write -> write.tryFail(new IOException("connection reset by peer")));
      }
    }

    byte[] bytes() {
      ByteArrayOutputStream all = new ByteArrayOutputStream();
      synchronized (written) {
        written.forEach(buffer -> all.writeBytes(buffer.getBytes()));
      }
      return all.toByteArray();
    }

    @Override
    public WriteStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
      return this;
    }

    @Override
    public Future<Void> write(Buffer data) {
      written.add(data);
      if (stallAfterEachWrite) {
        full = true;
      }
      if (holdWrites) {
        Promise<Void> inFlight = Promise.promise();
        held.add(inFlight);
        return inFlight.future();
      }
      return Future.succeededFuture();
    }

    @Override
    public void write(Buffer data, Handler<AsyncResult<Void>> handler) {
      write(data).onComplete(handler);
    }

    @Override
    public void end(Handler<AsyncResult<Void>> handler) {
      handler.handle(Future.succeededFuture());
    }

    @Override
    public WriteStream<Buffer> setWriteQueueMaxSize(int maxSize) {
      return this;
    }

    @Override
    public boolean writeQueueFull() {
      return full;
    }

    @Override
    public WriteStream<Buffer> drainHandler(Handler<Void> handler) {
      this.drainHandler = handler;
      return this;
    }
  }
}
