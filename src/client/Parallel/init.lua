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

local function findScriptFromSource(path: string)
    -- Path is "ServerScriptService.ScriptName"
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
    -- local sharedTable = SharedTableRegistry:GetSharedTable(name)

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
        partitions[i] = table.create(100)
    end

    local componentCount = 0
    local components = {}
    local partitionsDone = 0
    local partitionIndex = 0

    local running = coroutine.running()
    local connection = actor:BindToMessageParallel("receive", function(id: number, partition: { any })
        debug.profilebegin("Partition Received")
        local columns_map = nil
        for i = 1, #partition, componentCount + 1 do
            local entity = partition[i]
            local record = jecs.entity_index_try_get(world.entity_index, entity) :: jecs.Record
            if not record then continue end
            if not columns_map then
                columns_map = record.archetype.columns_map
            end
            
            columns_map[components[1]][record.row] = partition[i + 1]
            -- for j = 1, componentCount do
            --     columns_map[components[j]][record.row] = partition[i + j]
            -- end
        end
        partitionsDone += 1
        debug.profileend()
        if partitionsDone >= partitionIndex then
            task.synchronize()
            task.spawn(running)
        end
    end)

    return function<T...>(query: jecs.Query<T...>, callback: ModuleScript)
        debug.profilebegin("Intro Work")
        local inner = (query :: any) :: jecs.QueryInner
        
        running = coroutine.running()
        partitionsDone = 0
        components = inner.ids
        componentCount = #inner.ids
        
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

        local count = countQuery(query :: any)
        if count == 0 then
            debug.profileend()
            return
        end
        debug.profileend()

        -- loop through query and split into partitions
        debug.profilebegin("Partition Creation")
        local distribution = math.max(math.ceil(count / #work_group), 10)
        partitionIndex = 1
        local partitionCount = 0
        local partition = partitions[partitionIndex]

        for entity, a, b in query do
            local offset = partitionCount * (componentCount + 1) + 1
            partition[offset] = entity
            partition[offset + 1] = a
            partition[offset + 2] = b
            partitionCount += 1

            if partitionCount >= distribution then
                debug.profilebegin("Send Partition")
                work_group[partitionIndex]:SendMessage("run", actor, callback, componentMap, partitionCount, partition)
                partitionIndex += 1
                partitionCount = 0
                partition = partitions[partitionIndex]
                debug.profileend()
            end
        end
        if partitionCount > 0 then
            debug.profilebegin("Send Partition")
            work_group[partitionIndex]:SendMessage("run", actor, callback, componentMap, partitionCount, partition)
            debug.profileend()
        else
            partitionIndex -= 1
        end
        debug.profileend()

        coroutine.yield(running)
    end
end
