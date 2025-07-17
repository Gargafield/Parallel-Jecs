local SharedTableRegistry = game:GetService("SharedTableRegistry")

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

return function(workers: number, world: jecs.World)
    local work_group = {}

    -- get some unique name for the work group
    local name = tostring(work_group)
    local sharedTable = SharedTableRegistry:GetSharedTable(name)

    for i = 1, workers do
        local worker = Worker:Clone()
        worker.Name = name .. "_" .. i
        worker.Parent = script;
        (worker:FindFirstChild("Worker") :: LocalScript).Enabled = true
        table.insert(work_group, worker)
    end

    task.defer(function()
        for i, worker in work_group do
            worker:SendMessage("init", name, i)
        end
    end)

    local loaded : { [ModuleScript]: boolean? } = {}

    return function<T...>(query: jecs.Query<T...>, callback: ModuleScript)
        debug.profilebegin("Intro Work")
        local inner = (query :: any) :: jecs.QueryInner
        local running = coroutine.running()
        
        if not loaded[callback] then
            loaded[callback] = true

            for _, worker in work_group do
                worker:SendMessage("load", callback)
            end
        end

        local components = {}
        local componentCount = #inner.ids
        for i, componentId in inner.ids do
            components[i] = {
                componentId,
                world:get(componentId, jecs.Name) :: any
            }
        end

        local count = countQuery(query :: any)
        if count == 0 then
            debug.profileend()
            return
        end
        debug.profileend()

        -- loop through query and split into partitions
        debug.profilebegin("Partition Creation")
        local distribution = math.ceil(count / #work_group)
        local partiationIndex = 1
        local paritionCount = 0
        local partition = table.create(distribution) :: any
        if componentCount == 2 then
            for entity, a, b in query do
                if paritionCount >= distribution then
                    debug.profilebegin("Partition Flush")
                    partition["count"] = paritionCount
                    sharedTable[partiationIndex] = SharedTable.new(partition)
                    paritionCount = 0
                    partiationIndex += 1
                    table.clear(partition)
                    debug.profileend()
                end
                
                debug.profilebegin("Partition Add")
                local offset = paritionCount * (componentCount + 1)
                partition[offset] = entity
                partition[offset + 1] = a
                partition[offset + 2] = b
                paritionCount += 1
                debug.profileend()
            end
            debug.profilebegin("Partition Flush")
            partition["count"] = paritionCount
            sharedTable[partiationIndex] = SharedTable.new(partition)
            debug.profileend()
        end
        debug.profileend()

        debug.profilebegin("Schedule Parallel Work")
        for i = 1, partiationIndex do
            work_group[i]:SendMessage("run", callback, components)
        end
        debug.profileend()

        task.delay(0, function()
            -- synchronize with workers
            debug.profilebegin("Synchronize Workers")
            for i = 1, partiationIndex do
                local partition = sharedTable[i]
                local count = partition["count"]

                for j = 1, count do
                    local offset = (j - 1) * (componentCount + 1)
                    local entity = partition[offset]

                    for k = 1, componentCount do
                        local component = partition[offset + k]
                        world:set(entity, components[k][1], component)
                    end
                end
            end
            debug.profileend()

            coroutine.resume(running)
        end)
        coroutine.yield(running)
    end
end
