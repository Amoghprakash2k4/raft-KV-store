package com.projects.raftkv.dashboard;

import com.projects.raftkv.bench.ResultStore;
import java.awt.Desktop;
import java.io.UncheckedIOException;
import java.net.BindException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Starts the Raft KV Lab dashboard on http://localhost:8090 and opens it in the browser.
 *
 * <p>Run it either way:
 * <ul>
 *   <li>IntelliJ: open this file and press the green ▶ next to {@code main};</li>
 *   <li>terminal, from the project folder: {@code ./gradlew :raft-dashboard:run}.</li>
 * </ul>
 * Stop it with the red ■ (IntelliJ) or Ctrl+C (terminal).
 *
 * <p>Settings (environment variables, all optional): {@code RAFT_DASHBOARD_PORT} (default 8090),
 * {@code RAFT_DASHBOARD_NO_BROWSER=true} to not open a browser tab. Saved runs go to {@code results/} in the
 * project folder, wherever the program is started from.
 */
public final class MainApplication {
    private MainApplication() {}

    public static void main(String[] args) throws InterruptedException {
        int port = Integer.parseInt(System.getenv().getOrDefault("RAFT_DASHBOARD_PORT", "8090"));
        Path results = projectRoot().resolve("results");

        DashboardServer server;
        try {
            server = DashboardServer.start(port, new ResultStore(results));
        } catch (UncheckedIOException e) {
            if (e.getCause() instanceof BindException) {
                System.err.println("Port " + port + " is already in use: the dashboard is probably running already."
                        + "\nOpen http://localhost:" + port + " or stop the other copy first"
                        + " (or start this one with RAFT_DASHBOARD_PORT=8091).");
                System.exit(1);
            }
            throw e;
        }

        String url = "http://localhost:" + server.port();
        System.out.println();
        System.out.println("  Raft KV Lab is live:  " + url);
        System.out.println("  Saved runs:           " + results);
        System.out.println("  Stop with Ctrl+C (terminal) or the red stop button (IntelliJ).");
        System.out.println();
        Runtime.getRuntime().addShutdownHook(new Thread(server::close, "dashboard-shutdown"));

        if (!Boolean.parseBoolean(System.getenv().getOrDefault("RAFT_DASHBOARD_NO_BROWSER", "false"))) openBrowser(url);
        Thread.currentThread().join();   // keep running until stopped
    }

    /** The folder with settings.gradle.kts, searched upwards from the working directory (IntelliJ may start in a module). */
    static Path projectRoot() {
        Path dir = Path.of("").toAbsolutePath();
        for (Path p = dir; p != null; p = p.getParent()) {
            if (Files.exists(p.resolve("settings.gradle.kts"))) return p;
        }
        return dir;
    }

    private static void openBrowser(String url) {
        try {
            if (Desktop.isDesktopSupported() && Desktop.getDesktop().isSupported(Desktop.Action.BROWSE)) {
                Desktop.getDesktop().browse(URI.create(url));
                return;
            }
            String os = System.getProperty("os.name").toLowerCase();
            if (os.contains("mac")) new ProcessBuilder("open", url).start();
            else if (os.contains("linux")) new ProcessBuilder("xdg-open", url).start();
        } catch (Exception e) {
            System.out.println("  (Could not open a browser automatically; open " + url + " yourself.)");
        }
    }
}
