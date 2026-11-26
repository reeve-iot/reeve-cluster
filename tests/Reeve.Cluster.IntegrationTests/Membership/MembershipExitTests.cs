using Microsoft.Extensions.Hosting.Internal;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Reeve.Cluster;
using Reeve.Cluster.Core;
using Reeve.Cluster.Harness.Timing;
using Reeve.Cluster.Membership;

namespace Int.Membership;

public sealed class MembershipExitTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task S1_the_gates_exit_hand_off_and_the_hosts_own_stop_do_not_deadlock()
    {
        await Task.Run(async () =>
        {
            var options = new ClusterOptions { ConnectionString = "Host=unused", ReplicaId = "r1" };
            TestProfile.Apply(options);
            var wrapped = Options.Create(options);
            var gate = new LocalPhaseGate(wrapped, TimeProvider.System, NoModelEvents.Instance, NullLogger<LocalPhaseGate>.Instance);
            var lifetime = new ApplicationLifetime(NullLogger<ApplicationLifetime>.Instance);
            var store = new RefusingStore();
            var loop = new RenewalLoop(gate, store, wrapped, TimeProvider.System, NoFaultPoints.Instance, NoModelEvents.Instance, NullLogger<RenewalLoop>.Instance);
            var driver = new MembershipDriver(
                gate, store, loop, new NoMemberLoad(), new NoMemberActivation(),
                new DrainCoordinator(gate, [], store, wrapped, TimeProvider.System, NoFaultPoints.Instance, NoModelEvents.Instance, NullLogger<DrainCoordinator>.Instance),
                MemberIdentity.ForThisProcess(), new ClusterCapabilitiesRegistry(new NoCapabilityStore(), gate, wrapped, TimeProvider.System), lifetime, wrapped, TimeProvider.System,
                NoFaultPoints.Instance, NullLogger<MembershipDriver>.Instance);

            var inCallback = new ManualResetEventSlim();
            var gateHeld = new ManualResetEventSlim();
            var gateHeldInTime = false;
            lifetime.ApplicationStopping.Register(() =>
            {
                inCallback.Set();
                gateHeldInTime = gateHeld.Wait(Wait);
                _ = gate.Epoch;
            });

            gate.Subscribe(e =>
            {
                if (e.To == ReplicaPhase.Exited)
                {
                    gateHeld.Set();
                }
            });

            var stopping = new CancellationTokenSource();
            var run = driver.RunAsync(stopping.Token);
            await store.JoinCalled.WaitAsync(Wait);

            var hostStop = Task.Factory.StartNew(lifetime.StopApplication, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
            Assert.True(inCallback.Wait(Wait), "the host's stop did not run its callback");

            var fault = Task.Factory.StartNew(() => gate.ReportFatalFault("disk full"), CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);

            var both = Task.WhenAll(hostStop, fault);
            var finished = await Task.WhenAny(both, Task.Delay(Wait)) == both;
            stopping.Cancel();
            Assert.True(
                finished,
                "deadlock: StopApplication (holding its lock) waits for the gate's lock in its callback, while the gate's exit "
                + "hand-off (holding the gate's lock) waits in StopApplication");
            Assert.True(gateHeldInTime, "the gate's exit never reached the callback while the callback waited for it");
            await run.WaitAsync(Wait);
        });
    }

    private sealed class NoCapabilityStore : ICapabilityStore
    {
        public Task<bool> WriteAsync(string replicaId, long epoch, IReadOnlyList<string> capabilities, CancellationToken cancellationToken) =>
            Task.FromResult(false);
    }

    private sealed class RefusingStore : IMemberStore
    {
        private readonly TaskCompletionSource joinCalled = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task JoinCalled => joinCalled.Task;

        public Task<JoinAnswer?> JoinAsync(JoinRequest request, CancellationToken cancellationToken)
        {
            joinCalled.TrySetResult();
            return Task.FromResult<JoinAnswer?>(null);
        }

        public Task<RenewalAnswer> RenewAsync(string replicaId, long epoch, TimeSpan leaseTtl, CancellationToken cancellationToken) =>
            Task.FromException<RenewalAnswer>(new InvalidOperationException("no renewal in this test"));

        public Task<bool> WriteDrainingAsync(string replicaId, long epoch, CancellationToken cancellationToken) => Task.FromResult(true);

        public Task<bool> ClearServingSinceAsync(string replicaId, long epoch, CancellationToken cancellationToken) => Task.FromResult(true);
    }
}
