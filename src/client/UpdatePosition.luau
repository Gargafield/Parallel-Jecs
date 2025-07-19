--!native
--!optimize 2
--!strict

local jecs = require("@Packages/Jecs")

local deltaTime = 1 / 60

return function(world: jecs.World, components: { [string]: jecs.Entity }, query: () -> (jecs.Entity, vector, vector))
    debug.profilebegin("Update")
    local Position = components["Position"]
    for entity, position, velocity in query do
        if position.y > 255 then
            position = vector.create(position.x, 0)
        end
        workspace:Raycast(Vector3.zero, Vector3.yAxis * 100)
        world:set(entity, Position, position + velocity * deltaTime)
    end
    debug.profileend()
end
