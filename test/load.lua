math.randomseed(os.time())

local thread_id_counter = 1

function setup(thread)
   thread:set("thread_id", thread_id_counter)
   thread_id_counter = thread_id_counter + 1
end

function RandomString(length)
   length = length or 1
   if length < 1 then return nil end
   local array = {}
   for i = 1, length do
      array[i] = string.char(math.random(97, 122)) .. thread_id
   end
   return table.concat(array)
end

request = function()
   wrk.method = "POST"
   path = "/cns?key=" .. RandomString(22) .. "&duration=60s"
   wrk.headers["Content-Type"] = 'application/x-www-form-urlencoded'
   return wrk.format(nil, path)
end