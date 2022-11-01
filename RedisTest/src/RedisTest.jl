module RedisTest
include("Frames.jl")
using Redis
using Downloads
using .Frames

function run()
   # Downloads.download("https://raw.githubusercontent.com/curran/data/gh-pages/airbnb/airbnb_session_data.txt","data.txt")
#     conn = RedisConnection() 
#    lines = readlines("data.txt")

#    set(conn, "dataset.size", string(length(lines)))
# @show string(length(lines))
#    @time set(conn, "dataset.features",lines[1])
#    @time lpush(conn, "dataset.observations",lines[2:end])

   Frames.run()
end


end # module

