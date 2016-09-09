package org.neuinfo.foundry.common.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by bozyurt on 5/18/15.
 */
public class Profiler {
    private boolean profile = true;
    private boolean verbose = false;
    private Map<String, Stats> threads = new HashMap<String, Stats>(31);
    private static Map<String, Profiler> instanceMap = new HashMap<String, Profiler>(7);

    public synchronized static Profiler getInstance(String instanceId) {
        Profiler p = instanceMap.get(instanceId);
        if (p == null) {
            p = new Profiler();
            instanceMap.put(instanceId, p);
        }
        return p;
    }

    private Profiler() {
    }

    /**
     * Enables or disables profiling.
     *
     * @param value true if profiling is enabled
     */
    public void setProfiler(boolean profile) {
        this.profile = profile;
    }

    /**
     * Given a user specified key, creates or gets a new Stats object and starts
     * the timer for profiling. The key is used to identify the thread of
     * execution that is profiled.
     *
     * @param key a user specified unique identifier for the current thread of
     *            execution. This key allows to differentiate between different
     *            parts and threads of execution profiled.
     */
    public void entryPoint(String key) {
        if (!profile)
            return;
        Stats stats = threads.get(key);
        if (stats == null)
            threads.put(key, new Stats(System.currentTimeMillis(), key));
        else
            stats.enterTime = System.currentTimeMillis();
    }

    public void exitPoint(String key) {
        exitPoint(key, null);
    }

    /**
     * Given a user specified key and a reminder message, indicates the end of
     * profiling for a block of code. It shows the descriptive statistics for the
     * accumulative profiling for the code block of code between
     * <code>entryPoint()</code> and <code>exitPoint()</code> calls.
     *
     * @param key a user specified unique identifier for the current thread of
     *            execution. This key allows to differentiate between different
     *            parts and threads of execution profiled.
     * @param msg a reminder for the user to identify and make sense of the
     *            profiling stats printed.
     */
    public void exitPoint(String key, String msg) {
        if (!profile)
            return;
        long now = System.currentTimeMillis();
        Stats stats = threads.get(key);
        if (stats != null) {
            if (verbose) {
                if (msg == null)
                    msg = key;
                System.out.println("PROFILER>>" + msg + " elapsed time:"
                        + (now - stats.enterTime));
            }
            stats.updateStats(now - stats.enterTime);
        }
    }

    /**
     * For each code block profiled, shows the profiling statistics collected so
     * far.
     */
    public void showStats() {
        if (!profile) {
            return;
        }
        System.out
                .println("--------------- PROFILER STATISTICS -----------------");
        for (Iterator<Stats> it = threads.values().iterator(); it.hasNext(); ) {
            Stats stats = it.next();
            if (stats != null)
                stats.show();
        }
    }

    public boolean isVerbose() {
        return verbose;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

}

/**
 * Helper class for <code>Profiler</code>.
 */
class Stats {
    double avg = 0, min = 0, max = 0;
    int count = 0;
    String threadName = null;
    long enterTime;

    public Stats(long enterTime, String threadName) {
        this.threadName = threadName;
        this.enterTime = enterTime;
    }

    /**
     * Given the elapsed time between profiling entry and exit points, updates
     * the average, min and maximum execution times.
     *
     * @param timeElapsed the elapsed time between profiling entry and exit points
     */
    public void updateStats(long timeElapsed) {
        if (count == 0) {
            min = timeElapsed;
            max = timeElapsed;
            avg = timeElapsed;
            ++count;
            return;
        }
        if (min > timeElapsed)
            min = timeElapsed;
        if (max < timeElapsed)
            max = timeElapsed;
        avg = (avg * count + timeElapsed) / (++count);
    }

    /**
     * Shows the accumulated profiling statistics.
     */
    public void show() {
        System.out.println("Performance Statistics for " + threadName);
        System.out.println("Min=" + min + " milliseconds, Max=" + max
                + " milliseconds, Avg= " + avg + " milliseconds. (" + count + ")");
        System.out.println("");
    }
}// ;
