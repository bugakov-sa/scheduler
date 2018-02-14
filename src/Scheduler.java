import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Scheduler extends Thread {

    private static final long LOOP_INTERVAL_MILLIS = 50;
    private static final long OLD_TIME_OFFSET_MILLIS = 1000;

    private class Task {
        Callable callable;
        Future future;

        Task(Callable callable) {
            this.callable = callable;
        }

        void execute() {
            future = executor.submit(callable);
        }
    }

    private final ExecutorService executor;

    public Scheduler(ExecutorService executor) {
        this.executor = executor;
    }

    /*
        Каждому моменту времени соответствует список задач. Задачи добавляются в список в порядке их поступления в систему.
        Используется ConcurrentHashMap, поскольку эта коллекция гарантирует, что операции merge и compute с одним ключом
        выполняются синхронно. То есть, операции с одним списком задач выполняются последовательно, а операции с разными
        списками - параллельно.
         */
    private final ConcurrentHashMap<LocalDateTime, List<Task>> tasks = new ConcurrentHashMap<>();

    /*
    Все моменты времени хранятся в сортированном множестве. Это сделано, чтобы не тратить время на сортровку ключей task
    Плюс такого решения - не нужно беспокоиться, что tasks.size() станет настолько большим, что из-за долгой сортировки
    будет тормозить планирование. То есть, задачи будут стартовать с опозданием не потому что пул потоков занят, а потому
    что между выборками из tasks актуальных задач проходит много времени (из-за долгой сортировки)
    Минус такого решения - приходится заботиться об удалении элементов из множества
     */
    private final ConcurrentSkipListSet<LocalDateTime> times = new ConcurrentSkipListSet<>();

    private volatile boolean isStop = false;

    @Override
    public void run() {
        while (!isStop) {
            try {
                processActualTasks();
                removeOldTimes();
                Thread.sleep(LOOP_INTERVAL_MILLIS);
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
        executor.shutdown();
    }

    /*
    Следим, чтобы все списки задач, у которых время старта уже наступило, выполнялись в порядке их появления в системе
     */
    public void processActualTasks() {
        NavigableSet<LocalDateTime> actualTimes = times.headSet(LocalDateTime.now(), true);
        for (LocalDateTime actualTime : actualTimes) {
            tasks.computeIfPresent(actualTime, (time, taskList) -> {
                Task firstTask = taskList.get(0);
                if (firstTask.future == null) {
                    firstTask.execute();
                } else if (firstTask.future.isDone()) {
                    taskList.remove(0);
                    if (taskList.isEmpty()) {
                        return null;
                    } else {
                        taskList.get(0).execute();
                    }
                }
                return taskList;
            });
        }
    }

    /*
    Удаляем из times моменты времени из прошлого, для которых все задачи уже выполнены
    На эти моменты времени уже никогда не будут запланированы задачи (cм. комментарий к методу schedule)
    Поскольку состояние класса меняется из нескольких потоков, прошлым считаем только те моменты времени,
    которые < now - OLD_TIME_OFFSET_SECONDS
    Если же считать прошлым момент времени, который < now, возможна ситуация, когда из times будет удален момент времени,
    для которого есть список задач в tasks
     */
    public void removeOldTimes() {
        List<LocalDateTime> removingTimes = times.headSet(LocalDateTime.now().minusSeconds(OLD_TIME_OFFSET_MILLIS), false)
                .stream()
                .filter(time -> !tasks.contains(time))
                .collect(Collectors.toList());
        times.removeAll(removingTimes);
    }

    /*
    Если time < now, то задача добавляется в список, который соответствует моменту времени now
    Иначе - в список, который соответствует моменту времени time
    Таким образом, гарантируется, что списки, соответствующие моментам времени в прошлом, не могут расти
    Благодаря этой гарантии возможна реализация удаления из times тех моментов времени, на которые уже никогда не будут
    планироваться задачи - это удаление выполняется в методе removeOldTimes
     */
    public void schedule(LocalDateTime time, Callable task) {
        long t0 = System.currentTimeMillis();
        LocalDateTime timeToExecute = LocalDateTime.now().isAfter(time)
                ? LocalDateTime.now()
                : time;
        tasks.merge(
                timeToExecute,
                new ArrayList<>(Arrays.asList(new Task(task))),
                (callables, callables2) -> {
                    callables.addAll(callables2);
                    return callables;
                });
        times.add(timeToExecute);
        long t1 = System.currentTimeMillis();
        long duration = t1-t0;
        if(duration >= OLD_TIME_OFFSET_MILLIS) {
            /*
            Работа метода removeOldTimes основана на предположении, что метод, добавляющий элементы в times,
            выполняется меньше, чем за OLD_TIME_OFFSET_MILLIS
            Если это не так, то метод removeOldTimes может удалить из times тот момент времени, для которого
            в tasks есть задачи
             */
            System.err.println("Долгое выполнение метода schedule -  какие-то задачи могут быть никогда не выполнены");
        }

    }

    void stopNow() {
        isStop = true;
    }
}
