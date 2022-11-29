package aeron.and.sbe;


import baseline.BondEncoder;
import baseline.BooleanType;
import baseline.MessageHeaderEncoder;
import baseline.Rating;
import io.aeron.Publication;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.UnsafeBuffer;

public class SendAgent implements Agent {

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

  private final Publication publication;
  private final int sendCount;
  private final UnsafeBuffer buffer;
  private final BondEncoder encoder;
  private final MessageHeaderEncoder headerEncoder;
  private int currentCountItem = 0;

  public SendAgent(Publication publication, BondEncoder bondEncoder,
      MessageHeaderEncoder msgHeaderEncoder, int sendCount) {
    this.publication = publication;
    this.sendCount = sendCount;
    this.buffer = new UnsafeBuffer(ByteBuffer.allocate(4096));
    this.encoder = bondEncoder;
    this.headerEncoder = msgHeaderEncoder;
  }

  @Override
  public int doWork() {
    if (++currentCountItem > sendCount) {
      return 0;
    }

    if (publication.isConnected()) {
      if (publication.offer(buffer, 0, encode(encoder, buffer, headerEncoder)) > 0) {
        System.out.println("[" + Thread.currentThread().getName() + "] ::: " + currentCountItem);
      }
    }
    return 0;
  }

  public int encode(final BondEncoder bond, final UnsafeBuffer directBuffer,
      final MessageHeaderEncoder messageHeaderEncoder) {
    bond.wrapAndApplyHeader(directBuffer, 0, messageHeaderEncoder)
        .serialNumber(1230 + currentCountItem)
        .expiration(2025 + currentCountItem)
        .available(BooleanType.T)
        .rating(Rating.B)
        .putCode(CODE, 0)
        .putSomeNumbers(currentCountItem * 10, currentCountItem * 20, currentCountItem * 30,
            currentCountItem * 40)
        .putDesc(DESC, 0, DESC.capacity());

    return MessageHeaderEncoder.ENCODED_LENGTH + bond.encodedLength();
  }

  @Override
  public String roleName() {
    return "SENDER";
  }
}