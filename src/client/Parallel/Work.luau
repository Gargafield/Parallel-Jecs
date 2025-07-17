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

    actor:BindToMessageParallel("run", function(module: ModuleScript, components: { [number]: { number | string} })
        -- print(`{id}: Running module {module.Name}`)
        local func = modules[module]
        if not func then
            error("Module not loaded: " .. module.Name)
        end

        local componentCount = #components
        local partition = sharedTable[id]
        local count = partition["count"]

        local componentsMap = {}
        for i, component in components do
            componentsMap[component[2]] = i
        end
        
        local world = {
            set = function(self, entity: number, component, value: any)
                partition[entity + component] = value
            end,
        }

        local index = 0
        func(world, componentsMap, function(): any
            if index >= count then
                return nil
            end

            local offset = index * (componentCount + 1)
            index += 1

            return offset, partition[offset + 1], partition[offset + 2]
        end)
    end)
end