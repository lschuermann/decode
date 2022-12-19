defmodule RegistryExTest do
  defmodule RegistryExTest.TestServer do
    use GenServer

    @impl true
    def init(nil) do
      {:ok, nil}
    end

    @impl true
    def handle_info(info, state) do
      IO.puts("Received info: #{inspect(info)}")
      {:noreply, state}
    end
  end

  def run do
    # Start the server which will receive callbacks:
    {:ok, _} = GenServer.start_link(RegistryExTest.TestServer, nil, name: RegistryExTest.Listener)

    {:ok, _} =
      RegistryEx.start_link(
        keys: :unique,
        name: RegistryExTest.Reg,
        listeners: [RegistryExTest.Listener]
      )

    # Now, start a process and register it under a given key with the registry:
    {:ok, test_pid} =
      GenServer.start(
        RegistryExTest.TestServer,
        nil,
        name: {:via, RegistryEx, {RegistryExTest.Reg, "abc"}}
      )

    # Now, send a message to the test GenServer which makes it crash:
    GenServer.cast(test_pid, :hello)

    # Wait for a little.
    :timer.sleep(1000)

    IO.puts("Hello!")
  end
end

RegistryExTest.run()
