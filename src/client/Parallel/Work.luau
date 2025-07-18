local SharedTableRegistry = game:GetService("SharedTableRegistry")

return function(actor: Actor)
    local name: string
    local id: number
    local sharedTable : SharedTable

    local modules = {}

    actor:BindToMessage("init", function(_name: string, _id: number)
        print(`{_id}: Initializing worker {_name}`)
        name = _name
        id = _id
        sharedTable = SharedTableRegistry:GetSharedTable(_name)
    end)

    actor:BindToMessage("load", function(module: ModuleScript)
        print(`{id}: Loading module {module.Name}`)
        if modules[module] then return end
        modules[module] = require(module) :: any
    end)

    actor:BindToMessageParallel("run", function(caller: Actor, module: ModuleScript, componentMap: { [string]: number }, count: number, partition: { any })
        -- print(`{id}: Running module {module.Name}`)
        local func = modules[module]
        if not func then
            error("Module not loaded: " .. module.Name)
        end

        local componentCount = 0
        for _, _ in componentMap do
            componentCount += 1
        end
        
        local world = {
            set = function(self, entity: number, component, value: any)
                partition[entity + component] = value
            end,
        }

        local index = 0
        func(world, componentMap, function(): any
            if index >= count then
                return nil
            end

            local offset = index * (componentCount + 1) + 1
            index += 1

            return offset, partition[offset + 1], partition[offset + 2]
        end)

        caller:SendMessage("receive", id, partition)
    end)
end