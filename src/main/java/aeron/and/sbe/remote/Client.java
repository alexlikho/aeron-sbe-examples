package aeron.and.sbe.remote;

import static aeron.and.sbe.remote.Constants.SEND_COUNT;
import static aeron.and.sbe.remote.Constants.STREAM_ID;
import static org.agrona.CloseHelper.quietClose;

import baseline.BondEncoder;
import baseline.BooleanType;
import baseline.MessageHeaderEncoder;
import baseline.Rating;
import io.aeron.Aeron;
import io.aeron.Aeron.Context;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import org.agrona.CloseHelper;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.UnsafeBuffer;

public class Client implements Agent {
  private static final BondEncoder MSG_ENCODER = new BondEncoder();
  private static final MessageHeaderEncoder HEADER_ENCODER = new MessageHeaderEncoder();
  private static final byte[] CODE;
  private static final UnsafeBuffer DESC;

  static {
    try {
      CODE = "BOND07".getBytes(BondEncoder.codeCharacterEncoding());
      DESC = new UnsafeBuffer("USA GOV BOND [expiration=2025, discount=10%]".getBytes(
          BondEncoder.codeCharacterEncoding()));
    } catch (final UnsupportedEncodingException ex) {
      throw new RuntimeException(ex);
    }
  }

  private final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocate(4096));
  private final int sendCount;
  private final Aeron aeron;
  private State state;
  private Publication publication;
  private ShutdownSignalBarrier barrier;
  private int itemCount = 0;

  public Client(Aeron aeron, ShutdownSignalBarrier barrier, int sendCount) {
    this.aeron = aeron;
    this.barrier = barrier;
    this.sendCount = sendCount;
  }

  public static void main(String[] args) {
    var mediaDriverCtx = new MediaDriver.Context()
        .dirDeleteOnStart(true)
        .threadingMode(ThreadingMode.SHARED)
        .sharedIdleStrategy(new BusySpinIdleStrategy())
        .dirDeleteOnShutdown(true)
        .aeronDirectoryName("/dev/shm/lom-publisher");
    MediaDriver mediaDriver = MediaDriver.launchEmbedded(mediaDriverCtx);
    Aeron aeron = Aeron.connect(new Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()));

    var barrier = new ShutdownSignalBarrier();
    Client agent = new Client(aeron, barrier, SEND_COUNT);

    AgentRunner clientRunner = new AgentRunner(new BusySpinIdleStrategy(),
        Throwable::printStackTrace, null, agent);
    AgentRunner.startOnThread(clientRunner);

    barrier.await();

    CloseHelper.quietClose(clientRunner);
    CloseHelper.quietClose(aeron);
    CloseHelper.quietClose(mediaDriver);
  }

  @Override
  public void onStart() {
    System.out.println("################### CLIENT start ###################");
    state = State.AWAITING_OUTBOUND_CONNECT;
    publication = aeron.addExclusivePublication(Constants.SERVER_URI, STREAM_ID);
  }

  @Override
  public void onClose() {
    quietClose(publication);
  }

  @Override
  public int doWork() {
    switch (state) {
      case AWAITING_OUTBOUND_CONNECT -> {
        awaitConnected();
        state = State.READY;
      }
      case READY -> {
        sendMessage(++itemCount);
        if (itemCount >= sendCount) {
          state = State.STOP;
        }
      }
      case STOP -> barrier.signal();
    }
    return 0;
  }

  private void sendMessage(int itemCount) {
    long result = publication.offer(buffer, 0, encode(buffer));
    System.out.println(result > 0
        ? "[" + Thread.currentThread().getName() + "] ::: " + itemCount
        : "Aeron returned [" + result + "] on OFFER");
  }

  int encode(UnsafeBuffer buffer) {
    MSG_ENCODER.wrapAndApplyHeader(buffer, 0, HEADER_ENCODER)
        .serialNumber(1230 + itemCount)
        .expiration(2025 + itemCount)
        .available(BooleanType.T)
        .rating(Rating.B)
        .putCode(CODE, 0)
        .putSomeNumbers(itemCount * 10, itemCount * 20, itemCount * 30,
            itemCount * 40)
        .putDesc(DESC, 0, DESC.capacity());

    return MessageHeaderEncoder.ENCODED_LENGTH + MSG_ENCODER.encodedLength();
  }

  @Override
  public String roleName() {
    return "SENDER";
  }

  private void awaitConnected() {
    System.out.println("######### Awaiting outbound server connect #########");
    while (!publication.isConnected()) {
      aeron.context().idleStrategy().idle();
    }
  }

  enum State {
    AWAITING_OUTBOUND_CONNECT,
    READY,
    STOP
  }
}
