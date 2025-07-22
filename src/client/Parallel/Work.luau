--!optimize 2
--!native

local Serdes = require("@client/Parallel/Serdes")

return function(actor: Actor)
    local name: string
    local id: number

    local modules = {}

    actor:BindToMessage("init", function(_name: string, _id: number)
        print(`{_id}: Initializing worker {_name}`)
        name = _name
        id = _id
    end)

    actor:BindToMessage("load", function(module: ModuleScript)
        print(`{id}: Loading module {module.Name}`)
        if modules[module] then return end
        modules[module] = require(module) :: any
    end)

    local columns = {}
    actor:BindToMessageParallel("run", function(caller: Actor, module: ModuleScript, componentMap: { [string]: number }, count: number, types: { number }, buf: buffer)
        -- print(`{id}: Running module {module.Name}`)
        local func = modules[module]
        if not func then
            error("Module not loaded: " .. module.Name)
        end

        local componentCount = 0
        for _, _ in componentMap do
            componentCount += 1
        end

        Serdes.deserializeColumnsInto(columns, buf, types, count)
        
        local world = {
            set = function(self, entity: number, component, value: any)
                columns[component][entity] = value
            end,
        }

        local index = 1
        func(world, componentMap, function(): any
            if index > count then
                return nil
            end
            local entity = index
            index += 1

            return entity, columns[1][entity], columns[2][entity]
        end)

        Serdes.serializeColumnsInto(buf, columns, 1, count)

        caller:SendMessage("receive", id, buf)
    end)
end
