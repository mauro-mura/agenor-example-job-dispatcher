package jobs.manager;

import dev.agenor.core.Message;
import dev.agenor.core.annotations.Agent;
import dev.agenor.runtime.agent.BaseAgent;
import jobs.domain.JobSpec;

/** Puts jobs on {@code jobs.submitted} from inside a test, where there is no submit process. */
@Agent(type = "producer")
class TestProducer extends BaseAgent {

    private final String id;

    TestProducer(String id) {
        this.id = id;
    }

    @Override public String getAgentId() { return id; }
    @Override public String getAgentName() { return "Producer " + id; }

    void submit(JobSpec spec) {
        publish(spec);
    }

    /** Anything at all on the topic: what a careless producer would send. */
    void publish(Object content) {
        var message = Message.builder()
                .topic(JobManager.SUBMITTED_TOPIC)
                .senderId(id)
                .content(content)
                .build();
        getMessageDispatcher().publish(message).join();
    }
}
