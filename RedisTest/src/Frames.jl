module Frames

using  DataFrames:select, groupby,DataFrame,combine,ByRow,nrow,subset,first,sort,passmissing,transform
using Redis
using Pipe
using Dates
function run()

    conn = RedisConnection() 

    datasetSize = parse(Int, get(conn, "dataset.size"))

    features = @pipe get(conn, "dataset.features") |> split(_,"|")

    lines = lrange(conn, "dataset.observations",1,datasetSize - 1)

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

  @show names(df)

  #group and count agent
  @time @pipe select(df,[:id_visitor,:dim_user_agent]) |> groupby(_,:dim_user_agent) |> combine(_,nrow => :count)

  #Top 5 clients
  @time @pipe select(df,[:id_visitor]) |> groupby(_,:id_visitor) |> combine(_,nrow => :count) |> sort(_,:count,rev=true) |> first(_,5)

  #Sent a message vs sent a booking request by date sample of 100 latest 
  @time @pipe select(df,[:id_visitor,:sent_message,:sent_booking_request,:ds]) |> groupby(_,:ds) |> combine(_, :sent_booking_request => snt -> sum(snt),:sent_message => snt -> sum(snt)) |> sort(_,:ds, rev=true) |> first(_,100)

  #Sent a message vs sent
 @time @pipe select(df,[:id_visitor,:sent_message,:sent_booking_request,:ds]) |> combine(_, :sent_booking_request => snt -> sum(snt),:sent_message => snt -> sum(snt))

 @time @pipe select(df,[:id_visitor,:ts_min,:ts_max]) |> transform(_,[:ts_min,:ts_max] => (a,b) -> convert.(Dates.Minu, (b-a)))

end
    
end