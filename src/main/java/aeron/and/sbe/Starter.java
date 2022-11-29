package aeron.and.sbe;

import baseline.BondDecoder;
import baseline.BondEncoder;
import baseline.MessageHeaderDecoder;
import baseline.MessageHeaderEncoder;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;

public class Starter {

  public static void main(String[] args) {
    final String channel = "aeron:ipc";
    final int stream = 10;
    final int sendCount = 5;
    final var idleStrategy = new BusySpinIdleStrategy();
    final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();

    final MediaDriver.Context mediaDriverCtx = new MediaDriver.Context()
        .dirDeleteOnStart(true)
        .threadingMode(ThreadingMode.SHARED)
        .sharedIdleStrategy(new BusySpinIdleStrategy())
        .dirDeleteOnShutdown(true)
        .aeronDirectoryName("/dev/shm/lom1");
    final MediaDriver mediaDriver = MediaDriver.launchEmbedded(mediaDriverCtx);

    final Aeron.Context aeronCtx = new Aeron.Context()
        .aeronDirectoryName(mediaDriver.aeronDirectoryName());
    final Aeron aeron = Aeron.connect(aeronCtx);

    final Subscription subscription = aeron.addSubscription(channel, stream);
    final Publication publication = aeron.addPublication(channel, stream);

    // setup agents
    final SendAgent sendAgent = new SendAgent(publication, new BondEncoder(),
        new MessageHeaderEncoder(), sendCount);
    final ReceiveAgent receiveAgent = new ReceiveAgent(subscription, barrier, new BondDecoder(),
        new MessageHeaderDecoder(), sendCount);

    final AgentRunner sendAgentRunner = new AgentRunner(idleStrategy,
        Throwable::printStackTrace, null, sendAgent);
    final AgentRunner receiveAgentRunner = new AgentRunner(idleStrategy,
        Throwable::printStackTrace, null, receiveAgent);

    System.out.println("#################### RUNNER start ####################");

    // start runners
    AgentRunner.startOnThread(sendAgentRunner);
    AgentRunner.startOnThread(receiveAgentRunner);

    // wait for the final item to be received before closing
    barrier.await();

    // close resources
    receiveAgentRunner.close();
    sendAgentRunner.close();
    aeron.close();
    mediaDriver.close();
  }
}
