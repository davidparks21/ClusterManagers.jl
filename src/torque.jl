#export TorqueManager, launch

import Base: launch, manage, kill, init_worker, connect

# Usage example and description:
#   addprocs( TorqueManager(queue="normal", nodes=2, ppn=8) )
#
#   All parameters are optional to TorqueManager and come with the defaults documented below:
#     queue::AbstractString     - The queue to request, or use the default queue defined in torque if unspecified
#     nodes::Integer            - The # of nodes to have allocated (must be a simple integer value, choosing specific nodes is unsuported, see limitations), default=1
#     ppn::Integer              - Processes per node, the number of julia processes to execute on each node. If this needs to vary by node type, multiple calls to addprocs should be made, default=1
#     l::AbstractString         - Additional resource parameters specified with the -l option in qsub. Note that not all parameter combinations possible in qsub are supported, the parameters specified must work nicely with -t array_list which is used to deploy julia on each node in the job, default="", do not include the "-l", only the value
#     job_name::AbstractString  - Job name displayed in qstat, default="JuliaWorker"
#     priority::Integer         - Job priority, default=0 leaves it unspecified
#     env                       - An array of environment variable names, or name=value pairs, to pass to the workers or the symbol :ALL to send all variables
#     other_qsub_params:AbstractString - Other qsub parameters to add to the qsub command (must include the full parameter key and value such as "-W depend=..."), these will be added verbatim to the qsub command. No guarantees are made about these working, and any parameters used must be consistent with the use of -t array_list parameter.
#
# Limitations:
#   - Specific hosts cannot be specified (example: -l nodes=node1+node2), this does not work well with the -t array_list option, which is used to execute julia on each node.
#

immutable TorqueManager <: Base.ClusterManager
    queue::AbstractString
    nodes::Integer
    ppn::Integer
    l::AbstractString
    job_name::AbstractString
    priority::Integer
    env::Union{Array{AbstractString}, Symbol}
    other_qsub_params::AbstractString

    # Constructor that accepts all variable parameters with defaults
    TorqueManager(; queue::AbstractString="",
                    nodes::Integer=1,
                    ppn::Integer=1,
                    l::AbstractString="",
                    job_name::AbstractString="JuliaWorker",
                    priority::Integer=0,
                    env=Array{AbstractString}(0),
                    other_qsub_params::AbstractString=""  )=begin
      new(queue, nodes, ppn, l, job_name, priority, env, other_qsub_params)
    end
end


function launch(manager::TorqueManager, params::Dict, instances_arr::Array, c::Condition)
  try
    worker_arg = "--worker"  # TODO

    channel_port = Channel{Int64}(1)     # When the listener starts asynchronously it will set its listening port here
    channel_workers = Channel{AbstractString}(manager.nodes*manager.ppn)  # When the listener receives a worker notification it will put the result here
    server = @schedule network_listener(channel_port, channel_workers, manager.nodes*manager.ppn)
    master_port = take!(channel_port)

    # Define the qsub parameters
    qsub_t = ["-t", "1-$(manager.nodes)"]                                  # This defines an array request it will create a task per node.
    qsub_N = ["-N", "$(manager.job_name)"]                               # Job name
    qsub_l = ["-l", "nodes=1:ppn=$(manager.ppn)"]                          # Note that the # of nodes is controlled by qsub-t, this command is duplicated by the array request, so nodes should be fixed to 1 here.
    qsub_l2= ["-l", manager.l]
    qsub_q = (isempty(manager.queue)) ? [] : ["-q","$(manager.queue)"]    # If left blank we will let qsub default to it's normal queue
    qsub_j = ["-j", "oe"]                                                  # Join the stderr and stdout to one file
    qsub_p = (isempty(manager.queue)) ? [] : ["-p", "$(manager.priority)"] # Pass through optional priority parameter
    qsub_v = (typeof(manager.env)==Symbol && manager.env==:ALL) ?     # Environment variables to pass through to the worker nodes, can be -V for all variables if the user passes :ALL, or the user can specify a list of environment variables to pass
              "-V" : (length(manager.env) > 0) ?
              ["-v", "$(join(manager.env, ","))"] : []

    qsub_command  = `qsub $(qsub_t) $(qsub_N) $(qsub_l) $(qsub_l2) $(qsub_q) $(qsub_j) $(qsub_p) $(qsub_v) $(manager.other_qsub_params)`   # Simply qsub with the above parameters
    julia_command = "julia $(worker_arg) > /dev/tcp/$(getipaddr())/$(master_port)"                                              # Runs julia as a worker and pipes output over TCP back to this process
    bash_command  = "bash -c 'for i in {1..$(manager.ppn)}; do $(julia_command) & done; wait;'"                                 # Must run under bash because we use the bash network construct
    echo_command  = `echo $(bash_command)`

    #println("----------- DEBUG STATEMENTS ----------------")
    #println(qsub_command)
    #println(julia_command)
    #println(bash_command)
    #println(echo_command)
    #println("---------------------------------------------")

    # This qsub command will generate "ppn" julia processes on each node assigned by torque. The julia output (host and port information)
    # is redirected back to this processes via bash's /dev/tcp netowrk redirect mechanism.
    (stdout, qsub_proc) = open( pipeline( `echo $(bash_command)`, qsub_command ) )

    println( "Starting job: ", chomp(readline(stdout)) )

    # Take workers & register them as they're availalbe on channel_workers - the network listener will put the stdout of each worker on channel_workers as they communicate in.
    workercount = 0
    while(workercount < manager.nodes * manager.ppn)
      println("ready to receive workers")
      worker_stdout = take!(channel_workers)
      println("got a worker: ", worker_stdout)
      config = WorkerConfig()
      worker_info = match( r"julia_worker:(\d+)#([\d\.]+)", worker_stdout )
      config.host = worker_info.captures[2]
      config.port = parse( Int, worker_info.captures[1] )
      push!(instances_arr, config)
      notify(c)
      workercount += 1
    end

  catch e
      println("Error launching workers, caught exception: [$(e)]")
      println(e)
  end
end


function network_listener(channel_port::Channel, channel_workers::Channel, expected_connections::Integer)
  server = nothing
  master_port = 9009

  # Start the listener on port 9009, or higher if 9009 is unavailable, indicate the port number on the channel_port channel
  while true
    try
      server = listen(IPv4(0), master_port)
    catch err
      if typeof(err) == Base.UVError
        println("Port $(master_port) unavailable, increase port number to $(master_port+1)")
        master_port += 1
        continue        # couldn't open port, try again with the next port
      else
        rethrow(err)
      end # if
    end # catch

    println("Listener $(master_port) open")
    put!(channel_port, master_port)
    break   # We succeeded at opening the port, break out of the loop
  end # while

  sock = nothing
  # Accept connections
  try
    while expected_connections > 0
      sock = accept(server)
      expected_connections -= 1
      println("new accept")
      worker_startup_stdout = readline(sock)
      println("Received network input: ", chomp(worker_startup_stdout), "   ", typeof(worker_startup_stdout))
      put!(channel_workers, worker_startup_stdout)
    end
  finally
    println("Network listener shutdown")
    if sock != nothing && isopen(sock)
      close(sock)
    end
    if server != nothing && isopen(server)
      close(server)
    end
  end

end

function Base.manage(manager::TorqueManager, id::Int64, config::WorkerConfig, op::Symbol)
end

function Base.kill(manager::TorqueManager, id::Int64, config::WorkerConfig)
  remotecall(id,exit)
end
