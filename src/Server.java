public class Server {
    protected String serverName;
    protected int id;
    protected String state;
    protected int startTime;
    protected int core;
    protected int memory;
    protected int disk;
    protected int jobsWaiting;
    protected int jobsRunning;

    public Server(String sn, int id, String state, int st, int c, int m, int d, int jw, int jr) {
        this.serverName = sn;
        this.id = id;
        this.state = state;
        this.startTime = st;
        this.core = c;
        this.memory = m;
        this.disk = d;
        this.jobsWaiting = jw;
        this.jobsRunning = jr;
    }
}
