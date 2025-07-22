--!native
--!optimize 2
-- without, buffer operations are super slow

--[[
    The general idea is to split rows of an archetype into partitions and send them to
    parallel workers, which will then process them and return the result to the main thread.

    The structure of the archetype is still kept. The data sent to the workers contains the same
    column structure as the archetype. This means that `table.move` can be abused to
    move large amounts of data. When the data is to be joined back together, the workers will send
    their data back to the main thread, which will lastly stitch the data together.
    This is done by clearing the archetype columns and, again, abusing `table.move` to
    move the data back into the cleared columns. The keeps the column references intact.
--]]

local jecs = require("@Packages/Jecs")
local Serdes = require("@client/Parallel/Serdes")

local Worker : Actor = script:WaitForChild("Worker") :: Actor

local function countQuery<T...>(query: jecs.Query<T...>)
    local archetypes = query:archetypes()
    local count = 0
    for _, archetype in archetypes do
        count += #archetype.entities
    end
    return count
end

local function findScriptFromSource(path: string)
    local instance: any = game
    for path in path:gmatch("[^%.]+") do
        instance = instance:FindFirstChild(path)
        if instance == nil then
            error("Could not find script from source path: " .. path)
        end
    end
    return instance :: LuaSourceContainer
end

return function(workers: number, world: jecs.World)
    local work_group = {}

    local parent = script
    local script = findScriptFromSource(debug.info(2, "s"))
    local actor = script:FindFirstAncestorWhichIsA("Actor")

    if not actor then
        error("This function must be called from an Actor script.")
    end

    -- get some unique name for the work group
    local name = tostring(work_group)

    for i = 1, workers do
        local worker = Worker:Clone()
        worker.Name = name .. "_" .. i
        worker.Parent = parent;
        (worker:FindFirstChild("Worker") :: LocalScript).Enabled = true
        table.insert(work_group, worker)
    end

    task.defer(function()
        for i, worker in work_group do
            worker:SendMessage("init", name, i)
        end
    end)

    local loaded : { [ModuleScript]: boolean? } = {}

    local partitionsDone = 0
    local partitionNum = 0
    local recieved_partitions = {}
    local recieved_columns = {}
    local archetype : jecs.Archetype = nil
    local used_columns: { { any } } = {}
    -- local columns : { { { any } } } = {}

    local buffers, partitions, types

    local count = 0

    local running = coroutine.running()
    actor:BindToMessage("receive", function(id: number, buf: buffer)
        debug.profilebegin("Partition Received")
        recieved_partitions[id] = buf
        partitionsDone += 1
        debug.profileend()
        if partitionsDone >= partitionNum then
            debug.profilebegin("Join Partitions")
            for i = 1, partitionNum do
                Serdes.deserializeColumnsInto(recieved_columns, recieved_partitions[i], types, partitions[i][2], partitions[i][1])
            end
            for i = 1, #used_columns do
                table.clear(used_columns[i])
                table.move(
                    recieved_columns[i],
                    1,
                    count,
                    1,
                    used_columns[i]
                )
            end
            debug.profileend()

            task.synchronize()
            task.spawn(running)
        end 
    end)

    return function<T...>(query: jecs.Query<T...>, callback: ModuleScript)
        debug.profilebegin("Intro Work")
        local inner = (query :: any) :: jecs.QueryInner

        if #inner.compatible_archetypes > 1 then
            error("Query must have a single compatible archetype.")
        end

        running = coroutine.running()
        partitionsDone = 0
        partitionNum = 0
        
        if not loaded[callback] then
            loaded[callback] = true

            for _, worker in work_group do
                worker:SendMessage("load", callback)
            end
        end

        local componentMap = {}
        for i, componentId in inner.ids do
            componentMap[world:get(componentId, jecs.Name)] = i
        end

        count = countQuery(query :: any)
        if count == 0 then
            debug.profileend()
            return
        end
        debug.profileend()

        -- loop through query and split into partitions
        debug.profilebegin("Column Creation")
        archetype = inner.compatible_archetypes[1]

        table.clear(used_columns)
        for i = 1, #inner.ids do
            used_columns[i] = archetype.columns_map[inner.ids[i]]
            
            if not recieved_columns[i] then
                recieved_columns[i] = table.create(count)
            else
                table.clear(recieved_columns[i])
            end
        end

        debug.profileend()
        debug.profilebegin("Partition Creation")
        local distribution = math.max(math.ceil(count / #work_group), 10)
        buffers, partitions, types = Serdes.serializeColumnsFast(used_columns, count,  distribution)
        partitionNum = #partitions
        for i = 1, #buffers do
            work_group[i]:SendMessage("run", actor, callback, componentMap, partitions[i][2], types, buffers[i])
        end
        debug.profileend()

        coroutine.yield(running)
    end
end
