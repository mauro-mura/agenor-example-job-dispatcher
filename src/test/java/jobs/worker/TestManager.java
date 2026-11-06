package jobs.worker;

import dev.agenor.core.annotations.Agent;
import dev.agenor.core.dialogue.DialogueHandler;
import dev.agenor.core.dialogue.DialogueMessage;
import dev.agenor.core.dialogue.Performative;
import dev.agenor.runtime.agent.BaseAgent;
import dev.agenor.runtime.dialogue.DialogueCapability;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

/** Stands in for the manager: runs CFPs and awards, and keeps what comes back. */
@Agent(type = "manager")
public class TestManager extends BaseAgent {

    final BlockingQueue<DialogueMessage> informs = new LinkedBlockingQueue<>();
    final BlockingQueue<DialogueMessage> failures = new LinkedBlockingQueue<>();

    private final String id;
    private final DialogueCapability dialogue = new DialogueCapability(this);

    public TestManager(String id) {
        this.id = id;
    }

    @Override public String getAgentId() { return id; }
    @Override public String getAgentName() { return "Test manager " + id; }

    CompletableFuture<List<DialogueMessage>> cfp(String workerId, Object content) {
        return dialogue.callForProposals(List.of(workerId), content, Duration.ofSeconds(10));
    }

    CompletableFuture<Void> agree(DialogueMessage proposal, Object content) {
        return dialogue.reply(proposal, Performative.AGREE, content);
    }

    @DialogueHandler(performatives = Performative.INFORM)
    public void onInform(DialogueMessage msg) {
        informs.add(msg);
    }

    @DialogueHandler(performatives = Performative.FAILURE)
    public void onFailure(DialogueMessage msg) {
        failures.add(msg);
    }
}
