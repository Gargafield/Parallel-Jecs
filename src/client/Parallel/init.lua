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

    local partitions = {}
    for i = 1, workers do
        partitions[i] = {}
    end

    local partitionsDone = 0
    local partitionIndex = 0
    local recieved_partitions = {}
    local recieved_columns = {}
    local archetype : jecs.Archetype = nil
    local used_columns = {}
    local columns : { { { number } } } = {}
    local count = 0

    local running = coroutine.running()
    actor:BindToMessage("receive", function(id: number, partition: { any })
        debug.profilebegin("Partition Received")
        recieved_partitions[id] = partition
        partitionsDone += 1
        debug.profileend()
        if partitionsDone >= partitionIndex then
            debug.profilebegin("Join Partitions")
            for i = 1, partitionIndex do
                for j = 1, #used_columns do
                    table.move(
                        recieved_partitions[i][j],
                        1,
                        columns[j][i][2],
                        columns[j][i][1],
                        recieved_columns[j]
                    )
                end
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
        partitionIndex = 0
        
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
        end

        local distribution = math.max(math.ceil(count / #work_group), 10)
        for i = 1, #used_columns do
            if not columns[i] then
                columns[i] = {}
                for j = 1, workers do
                    columns[i][j] = { 1, 1 }
                end
            else
                for j = 1, workers do
                    columns[i][j][1] = 1
                    columns[i][j][2] = 1
                end
            end
            if not recieved_columns[i] then
                recieved_columns[i] = table.create(count)
            else
                table.clear(recieved_columns[i])
            end
        end
        debug.profileend()
        debug.profilebegin("Partition Creation")
        local counter = count
        for i = 1, workers do
            local localDistribution = math.min(counter, distribution)
            if localDistribution <= 0 then break end
            debug.profilebegin("Column Move")
            for j = 1, #used_columns do
                if not partitions[i][j] then
                    partitions[i][j] = table.create(localDistribution)
                else
                    table.clear(partitions[i][j])
                end
                local index = count - counter + 1
                
                table.move(
                    used_columns[j],
                    index,
                    index + localDistribution,
                    1,
                    partitions[i][j]
                )
                columns[j][i][1] = index
                columns[j][i][2] = localDistribution
            end
            counter -= localDistribution
            debug.profileend()
            partitionIndex += 1
            work_group[i]:SendMessage("run", actor, callback, componentMap, localDistribution, partitions[i])
        end
        debug.profileend()

        coroutine.yield(running)
    end
end
