--!native
--!optimize 2
--!strict

local PARALLEL = true
local WORKERS = 8

local Players = game:GetService("Players")
local RunService = game:GetService("RunService")

local jecs = require("@Packages/Jecs")
local Canvas = require("@shared/Canvas")

local UpdatePosition = (script.Parent :: any):WaitForChild("UpdatePosition") :: ModuleScript

local world = jecs.world()
local Position = world:component() :: jecs.Entity<vector>
local Velocity = world:component() :: jecs.Entity<vector>
local Color = world:component() :: jecs.Entity<number>
world:set(Position, jecs.Name, "Position")
world:set(Velocity, jecs.Name, "Velocity")
world:set(Color, jecs.Name, "Color")

local Colors = {
    0xFF0000FF, -- Red
    0x00FF00FF, -- Green
    0x0000FFFF, -- Blue
    0xFFFF00FF, -- Yellow
    0xFF00FFFF, -- Cyan
    0xFF00FF00, -- Magenta
    0xFFFFFFFF, -- White
    0x000000FF, -- Black
    0x808080FF, -- Gray
    0xFFA500FF, -- Orange
    0x800080FF, -- Purple
    0x008000FF, -- Dark Green
    0x000080FF, -- Navy
    0x808000FF, -- Olive
    0xFFC0CBFF, -- Pink
}

local canvas = Canvas.new(256, 256)

do
    local screenGui
    screenGui = Instance.new("ScreenGui")
    screenGui.Parent = Players.LocalPlayer.PlayerGui
    screenGui.IgnoreGuiInset = true
    screenGui.ResetOnSpawn = false

    canvas.instance.AnchorPoint = Vector2.new(0.5, 0.5)
    canvas.instance.Position = UDim2.new(0.5, 0, 0.5, 0)
    canvas.instance.BackgroundColor3 = Color3.new(0, 0, 0)
    canvas:Mount(screenGui)
end

local Parallel = require("@client/Parallel")

local maxCount = 10000
local spawnCount = math.max(math.round(maxCount / 666), 1)
local count = 0

if not PARALLEL then
    local deltaTime = 1 / 60
    while true do
        task.wait()
        local start = os.clock()
        canvas:Clear()
        debug.profilebegin("Render")
        debug.profilebegin("Query")
        local query = world:query(Position, Color)
        debug.profileend()
        for entity, position, color in query do
            canvas:SetPixel(math.clamp(math.floor(position.x), 0, 255), math.clamp(math.floor(position.y), 0, 255), color)
        end
        debug.profilebegin("Render")
        canvas:Render()
        debug.profileend()
        debug.profileend()

        if count < maxCount then
            debug.profilebegin("Spawn")
            for i = 1, math.min(spawnCount, maxCount - count) do
                local entity = world:entity()
                jecs.bulk_insert(world, entity, { Position, Velocity, Color }, {
                    vector.create(math.random(0, 255), 0),
                    vector.create(0, 25),
                    Colors[math.random(1, #Colors)]
                })
                count += 1
            end
            debug.profileend()
        end

        debug.profilebegin("Update")
        debug.profilebegin("Query")
        local query = world:query(Position, Velocity)
        debug.profileend()
        for entity, position, velocity in query do
            if position.y > 255 then
                position = vector.create(position.x, 0)
            end
            workspace:Raycast(Vector3.zero, Vector3.yAxis * 100)
            world:set(entity, Position, position + velocity * deltaTime)
        end
        debug.profileend()
        local stop = os.clock()
        print("Frame time:", (stop - start) * 1000)
        print(count)
    end
else
    local parallel = Parallel(WORKERS, world)

    while true do
        task.wait()
        local start = os.clock()
        canvas:Clear()
        debug.profilebegin("Render")
        debug.profilebegin("Query")
        local query = world:query(Position, Color)
        debug.profileend()
        for entity, position, color in query do
            canvas:SetPixel(math.clamp(math.floor(position.x), 0, 255), math.clamp(math.floor(position.y), 0, 255), color)
        end
        debug.profilebegin("Render")
        canvas:Render()
        debug.profileend()
        debug.profileend()


        if count < maxCount then
            debug.profilebegin("Spawn")
            for i = 1, math.min(spawnCount, maxCount - count) do
                local entity = world:entity()
                jecs.bulk_insert(world, entity, { Position, Velocity, Color }, {
                    vector.create(math.random(0, 255), 0),
                    vector.create(0, 25),
                    Colors[math.random(1, #Colors)]
                })
                count += 1
            end
            debug.profileend()
        end

        parallel(world:query(Position, Velocity), UpdatePosition)
        local stop = os.clock()
        print("Frame time:", (stop - start) * 1000)
        print(count)
    end
end

