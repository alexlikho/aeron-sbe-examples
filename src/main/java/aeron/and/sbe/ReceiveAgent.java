package aeron.and.sbe;

import baseline.BondDecoder;
import baseline.MessageHeaderDecoder;
import io.aeron.Subscription;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.UnsafeBuffer;

public class ReceiveAgent implements Agent {

  private final Subscription subscription;
  private final ShutdownSignalBarrier barrier;
  private final int sendCount;
  private final BondDecoder decoder;
  private final MessageHeaderDecoder headerDecoder;
  private int currentCountItem = 0;

  public ReceiveAgent(Subscription subscription, ShutdownSignalBarrier barrier,
      BondDecoder bondDecoder, MessageHeaderDecoder messageHeaderDecoder,
      int sendCount) {
    this.subscription = subscription;
    this.barrier = barrier;
    this.sendCount = sendCount;
    this.decoder = bondDecoder;
    this.headerDecoder = messageHeaderDecoder;
  }

  static void decode(
      final BondDecoder bond, final DirectBuffer directBuffer,
      final MessageHeaderDecoder headerDecoder, int offset, int msgNumber)
      throws Exception {

    bond.wrapAndApplyHeader(directBuffer, offset, headerDecoder);

    System.err.println(
        "//////////////////////// MSG TOP [" + msgNumber + "] ////////////////////////");
    final StringBuilder sb = new StringBuilder();
    sb.append("bond.serialNumber=").append(bond.serialNumber());
    sb.append("\nbond.expiration=").append(bond.expiration());
    sb.append("\nbond.available=").append(bond.available());
    sb.append("\nbond.rating=").append(bond.rating());
    sb.append("\nbond.code=").append(bond.code());

    sb.append("\nbond.someNumbers=");
    for (int i = 0, size = BondDecoder.someNumbersLength(); i < size; i++) {
      sb.append(bond.someNumbers(i)).append(", ");
    }

    final byte[] buffer = new byte[128];
    final UnsafeBuffer tempBuffer = new UnsafeBuffer(buffer);
    final int tempBufferLength = bond.getDesc(tempBuffer, 0, tempBuffer.capacity());
    sb.append("\nbond.desc=").append(
        new String(buffer, 0, tempBufferLength, BondDecoder.descCharacterEncoding()));

    System.err.println(sb);
    System.err.println(
        "//////////////////////// MSG END [" + msgNumber + "] ////////////////////////");
  }

  @Override
  public int doWork() {
    return subscription.poll(this::handler, 5);
  }

  private void handler(DirectBuffer buffer, int offset, int length, Header header) {
    if (++currentCountItem > sendCount) {
      barrier.signal();
    }

    System.out.println(
        "[" + Thread.currentThread().getName() + "] ::: message # : " + currentCountItem);
    try {
      decode(decoder, buffer, headerDecoder, offset, currentCountItem);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String roleName() {
    return "RECEIVER";
  }

}
