module RedisJl
using Redis, Downloads, Pipe, Dates, UUIDs
using  DataFrames:select, groupby,DataFrame,combine,ByRow,nrow,subset,first,sort,passmissing,transform

function run()
    conn = RedisConnection() 

    datasetSize = llen(conn, "source:dataset:observations")

    features = @pipe get(conn, "source:dataset:features") |> split(_,"|")

    lines = @time lrange(conn, "source:dataset:observations",1,datasetSize)

    obs = [split(line,"|") for line in lines]

    #@show features
    obsMtx = mapreduce(permutedims,vcat, obs)

    columns = [col for col in eachcol(obsMtx)]

    df = DataFrame(columns, features)

    #convert type
    df[!,:sent_booking_request] = passmissing(parse).(Int16, df[!,:sent_booking_request])
    df[!,:sent_message] = passmissing(parse).(Int16, df[!,:sent_message])
    df[!,:ts_min] = passmissing(DateTime).(df[!,:ts_min], "yyyy-mm-dd HH:MM:SS")
    df[!,:ts_max] = passmissing(DateTime).(df[!,:ts_max], "yyyy-mm-dd HH:MM:SS")

    #group and count agent
    group_and_count_agents = @time @pipe select(df,[:id_visitor,:dim_user_agent]) |> groupby(_,:dim_user_agent) |> combine(_,nrow => :count)  |> sort(_,:count,rev=true) |> first(_,10)

    #Top 5 clients
    top_5_clients = @time @pipe select(df,[:id_visitor]) |> groupby(_,:id_visitor) |> combine(_,nrow => :count) |> sort(_,:count,rev=true) |> first(_,5)

    #Sent a message vs sent a booking request by date sample of 100 latest 
    sent_messages_vs_sent_bookings_by_date = @time @pipe select(df,[:id_visitor,:sent_message,:sent_booking_request,:ds]) |> groupby(_,:ds) |> combine(_, :sent_booking_request => snt -> sum(snt),:sent_message => snt -> sum(snt)) |> sort(_,:ds, rev=true) |> first(_,100)

    #Sent a message vs sent
    sent_messages_vs_sent_bookings = @time @pipe select(df,[:id_visitor,:sent_message,:sent_booking_request,:ds]) |> combine(_, :sent_booking_request => snt -> sum(snt),:sent_message => snt -> sum(snt))

    #Time spent per session
    time_spent_per_session = @time @pipe select(df,:id_visitor,[:ts_min,:ts_max] => (a,b) -> convert.(Dates.Second, (b-a))) |> first(_,50)

    @time writeDftoRedis(conn,"group_and_count_agents",group_and_count_agents)
    #@time writeDftoRedis(conn,"top_5_clients",top_5_clients)
    #@time writeDftoRedis(conn,"sent_messages_vs_sent_bookings_by_date",sent_messages_vs_sent_bookings_by_date)
    #@time writeDftoRedis(conn,"sent_messages_vs_sent_bookings",sent_messages_vs_sent_bookings)
    #@time writeDftoRedis(conn,"time_spent_per_session",time_spent_per_session)

    @show smembers(conn, "report:list")
    @show smembers(conn, "report:time_spent_per_session:columns")

    #readDffromRedis(conn,"group_and_count_agents",["dim_user_agent","count"])
    #group_and_count_agents
    #group_and_count_agents
    #top_5_clients
    #sent_messages_vs_sent_bookings_by_date

    #clean Redis
   # @show del(conn, "source:dataset:observations", "source:dataset:features")
end

function writeDftoRedis(conn,key::String,df::DataFrame)
    
    labels::Vector{String} = []
    datasets::Dict{String, Vector{Int64}} = Dict()

    if key === "group_and_count_agents"
        datasetLabels = names(df)
        append!(labels, df[!,:dim_user_agent])
        datasets[datasetLabels[1]] = df[!,:count]
    elseif key === "top_5_clients"
        
    elseif key === "sent_messages_vs_sent_bookings_by_date"    

    elseif key === "sent_messages_vs_sent_bookings"

    elseif key === "time_spent_per_session"  
        
    end

    @show labels
    @show datasets

    cols::Vector{String} = []
    # for colName::String in names(df)
    #     lpush(conn, "report:$key:datasets", labels)
    #     push!(cols, colName)
    # end 

    sadd(conn, "report:list",key)

    for label in labels 
        sadd(conn, "report:$key:labels", label) 
    end
    
    for (datasetName, data) in datasets
        datasetKey = uuid4()
        hset(conn, "report:$key:datasets",datasetName, datasetKey)
        lpush(conn,datasetKey,data)
    end
end

function readDffromRedis(conn,key::String,columns::Vector{String})
    ds::Dict{String, Vector{String}} = Dict()
    
    length = llen(conn,"$key:$(columns[1])")

    for colName in columns
         data = lrange(conn, "$key:$colName",1,length)
         ds[colName] = data
    end 
    
    @show ds["dim_user_agent"][18]
    @show ds["count"][18]

   Frames.run()
end

end # module

