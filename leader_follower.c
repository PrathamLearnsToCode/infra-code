using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

class Follower
{
    public string Name { get; private set; }
    public List<string> Data { get; private set; }

    private static Random rand = new Random();

    public Follower(string name)
    {
        Name = name;
        Data = new List<string>();
    }

    public async Task ReplicateAsync(string data)
    {
        double delay = rand.NextDouble() * 1000;
        await Task.Delay((int)delay);
        Data.Add(data);
        Console.WriteLine($"{Name} replicated data: {data} after {delay:F2}ms");
    }
}

class Leader
{
    private List<Follower> followers;
    private List<string> log = new List<string>();

    public Leader(List<Follower> followers)
    {
        this.followers = followers;
    }

    public async Task WriteAsync(string data, string mode)
    {
        Console.WriteLine($"\n[Leader] Writing data: {data} in {mode.ToUpper()} mode");
        log.Add(data);

        switch (mode.ToLower())
        {
            case "async":
                Console.WriteLine("[Leader] Starting async replication");
                foreach (var follower in followers)
                {
                    _ = follower.ReplicateAsync(data); 
                }
                Console.WriteLine("[Leader] Async replication initiated");
                break;

            case "sync":
                Console.WriteLine("[Leader] Starting sync replication");
                var syncTasks = new List<Task>();
                foreach (var follower in followers)
                {
                    syncTasks.Add(follower.ReplicateAsync(data));
                }
                await Task.WhenAll(syncTasks);
                Console.WriteLine("[Leader] Sync replication completed");
                break;

            case "semi-sync":
                Console.WriteLine("[Leader] Starting semi-sync replication");
                var semiSyncTasks = new List<Task>();
                var cts = new CancellationTokenSource();
                int ackCount = 0;
                object lockObj = new object();

                foreach (var follower in followers)
                {
                    var task = Task.Run(async () =>
                    {
                        await follower.ReplicateAsync(data);
                        lock (lockObj)
                        {
                            ackCount++;
                            if (ackCount == 1)
                            {
                                cts.Cancel(); 
                            }
                        }
                    });
                    semiSyncTasks.Add(task);
                }

                try
                {
                    await Task.Delay(-1, cts.Token);
                }
                catch (OperationCanceledException) { }

                Console.WriteLine("[Leader] Semi-sync replication: 1 follower acknowledged");
                await Task.WhenAll(semiSyncTasks);
                break;

            default:
                Console.WriteLine("Unknown replication mode. Please use 'async', 'sync', or 'semi-sync'.");
                break;
        }
    }
}

class Program
{
    static async Task Main()
    {
        var f1 = new Follower("Follower-1");
        var f2 = new Follower("Follower-2");
        var f3 = new Follower("Follower-3");

        var leader = new Leader(new List<Follower> { f1, f2, f3 });

        await leader.WriteAsync("Write-1", "async");
        await Task.Delay(2000);

        await leader.WriteAsync("Write-2", "sync");

        await leader.WriteAsync("Write-3", "semi-sync");
    }
}
