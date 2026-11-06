package jobs.manager;

import jobs.service.ServiceMain;

import java.util.concurrent.CountDownLatch;

/** The manager as its own JVM, through the service's {@link ServiceMain}: killable and restartable. */
public final class ManagerMain {

    private ManagerMain() {}

    public static void main(String[] args) throws Exception {
        var service = ServiceMain.run(System.getenv());
        System.out.println("CHECKPOINT started");
        System.out.flush();
        Runtime.getRuntime().addShutdownHook(new Thread(service::close));
        new CountDownLatch(1).await();
    }
}
