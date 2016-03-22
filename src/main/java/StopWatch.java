import java.util.HashMap;

/**
 * @author Sebastian Schüpbach
 * @version 0.1
 *          <p>
 *          Created on 21.03.16
 */
public class StopWatch {

    private long startTime;
    private long lapTime;
    private int counter;
    private boolean hasStarted = false;


    public long start() {
        startTime = System.currentTimeMillis();
        lapTime = startTime;
        counter = 0;
        hasStarted = true;
        return startTime;
    }

    public boolean hasStarted() {
        return hasStarted;
    }

    public int lapNumber() {
        return counter;
    }

    public long lapTime() {
        long lastLapTime = lapTime;
        lapTime = System.currentTimeMillis();
        counter++;
        return delta(lapTime, lastLapTime);
    }

    public HashMap<String, Long> reset() {
        HashMap<String, Long> container = new HashMap<>();
        counter++;
        long stopTime = System.currentTimeMillis();
        long totalTime = stopTime - startTime;
        container.put("total", totalTime);
        container.put("avg", avg(stopTime, startTime, counter));
        counter = 0;
        return container;
    }

    public static long delta(long endTime, long startTime) {
        return endTime - startTime;
    }

    public static long avg(long endTime, long startTime, int count) {
        return delta(endTime, startTime) / count;
    }
}
