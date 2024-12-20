defmodule Sequin.TaskSupervisor do
  @moduledoc false
  def child_spec do
    {Task.Supervisor, name: __MODULE__}
  end
end
