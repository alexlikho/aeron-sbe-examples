package aeron.and.sbe.remote;

import static aeron.and.sbe.remote.Constants.SEND_COUNT;
import static aeron.and.sbe.remote.Constants.STREAM_ID;
import static org.agrona.CloseHelper.quietClose;

import baseline.BondDecoder;
import baseline.MessageHeaderDecoder;
import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.UnsafeBuffer;

public class Server implements Agent {
  private final static MessageHeaderDecoder HEADER_DECODER = new MessageHeaderDecoder();
  private final static BondDecoder MSG_DECODER = new BondDecoder();
  private final Aeron aeron;
  private final int sendCount;
  private Subscription subscription;
  private ShutdownSignalBarrier barrier;
  private int itemCount = 0;

  public Server(Aeron aeron, ShutdownSignalBarrier barrier, int sendCount) {
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
        .aeronDirectoryName("/dev/shm/lom-subscriber");
    var mediaDriver = MediaDriver.launchEmbedded(mediaDriverCtx);

    Aeron aeron = Aeron.connect(
        new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()));

    var barrier = new ShutdownSignalBarrier();
    var serverRunner = new AgentRunner(new BusySpinIdleStrategy(),
        Throwable::printStackTrace, null, new Server(aeron, barrier, SEND_COUNT));

    AgentRunner.startOnThread(serverRunner);

    // Await shutdown signal
    barrier.await();

    CloseHelper.quietClose(serverRunner);
    CloseHelper.quietClose(aeron);
    CloseHelper.quietClose(mediaDriver);
  }

  @Override
  public void onStart()
  {
    System.out.println("#################### SUBSCRIBER start ####################");
    subscription = aeron.addSubscription(Constants.SERVER_URI, STREAM_ID);
  }

  @Override
  public void onClose() {
    quietClose(subscription);
  }

  @Override
  public int doWork() {
    return subscription.poll(this::handler, 1);
  }

  private void handler(DirectBuffer buffer, int offset, int length, Header header) {
    System.out.println(
        "[" + Thread.currentThread().getName() + "] ::: message # : " + ++itemCount);
    try {
      decode(buffer, offset, itemCount);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    if (itemCount >= sendCount) {
      barrier.signal();
    }
  }

  @Override
  public String roleName() {
    return "RECEIVER";
  }

  void decode(DirectBuffer buffer, int offset, int msgNum) throws Exception {
    MSG_DECODER.wrapAndApplyHeader(buffer, offset, HEADER_DECODER);

    System.err.println(
        "//////////////////////// MSG TOP [" + msgNum + "] ////////////////////////");
    final StringBuilder sb = new StringBuilder();
    sb.append("bond.serialNumber=").append(MSG_DECODER.serialNumber());
    sb.append("\nbond.expiration=").append(MSG_DECODER.expiration());
    sb.append("\nbond.available=").append(MSG_DECODER.available());
    sb.append("\nbond.rating=").append(MSG_DECODER.rating());
    sb.append("\nbond.code=").append(MSG_DECODER.code());

    sb.append("\nbond.someNumbers=");
    for (int i = 0, size = BondDecoder.someNumbersLength(); i < size; i++) {
      sb.append(MSG_DECODER.someNumbers(i)).append(", ");
    }

    final byte[] temp = new byte[128];
    final UnsafeBuffer tempBuffer = new UnsafeBuffer(temp);
    final int tempBuffLength = MSG_DECODER.getDesc(tempBuffer, 0, tempBuffer.capacity());
    sb.append("\nbond.desc=").append(
        new String(temp, 0, tempBuffLength, BondDecoder.descCharacterEncoding()));

    System.err.println(sb);
    System.err.println(
        "//////////////////////// MSG END [" + msgNum + "] ////////////////////////");
  }
}
