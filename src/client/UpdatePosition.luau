--!native
--!optimize 2
--!strict

local jecs = require("@Packages/Jecs")

local deltaTime = 1 / 60

return function(world: jecs.World, components: { [string]: jecs.Entity }, query: () -> (jecs.Entity, vector, vector))
    debug.profilebegin("Update")
    local Position = components["Position"]
    for entity, position, velocity in query do
        local sum = vector.zero
        for i = 1, 10 do
            sum += velocity * deltaTime
        end
        world:set(entity, Position, position + velocity * deltaTime)
    end
    debug.profileend()
end
