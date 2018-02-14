import java.time.LocalDateTime;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    static class TestTask implements Callable {
        final int number;
        final LocalDateTime time;

        public TestTask(int number, LocalDateTime time) {
            this.number = number;
            this.time = time;
        }

        @Override
        public Object call() {
            System.out.println("now " + LocalDateTime.now() + " \ttask number " + number + " \ttime " + time);
            return null;
        }
    }

    /*
    Планируем задачи на моменты времени в прошлом, на настоящий момент времени и на моменты времени в будущем
    На каждый момент времени планируем две задачи
    При запуске программы видно, что задачи, запланированные на прошлое и настоящее, выполняются как можно скорее
    А задачи, запланированные на будущее, выполняются по расписанию
    Также видно, что задачи, запланированные на один момент времени, выполняются в порядке их создания
     */
    public static void main(String[] args) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        Scheduler scheduler = new Scheduler(executor);
        scheduler.start();
        int taskNumber = 0;
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime minTime = now.minusSeconds(3);
        LocalDateTime maxTime = now.plusSeconds(5);
        for (LocalDateTime time = minTime; time.isBefore(maxTime); time = time.plusSeconds(1)) {
            for (int i = 0; i < 2; i++) {
                scheduler.schedule(time, new TestTask(++taskNumber, time));
            }
        }
        TimeUnit.SECONDS.sleep(10);
        scheduler.stopNow();
    }
}
