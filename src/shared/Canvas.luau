local AssetService = game:GetService("AssetService")

local Canvas = {}
Canvas.__index = Canvas


function Canvas.new(width: number, height: number)
    local self = setmetatable({}, Canvas)
    
    self.width = width
    self.height = height
    self.size = Vector2.new(width, height)
    
    self.buffer = buffer.create(width * height * 4) -- Assuming 4 bytes per pixel (RGBA)
    self.image = AssetService:CreateEditableImage({ Size = self.size })
    self.instance = Instance.new("ImageLabel")
    self.instance.ImageContent = Content.fromObject(self.image)
    self.instance.Size = UDim2.new(0, width, 0, height)

    return self
end

export type Canvas = typeof(Canvas.new(1, 1))

function Canvas.Render(self: Canvas)
    self.image:WritePixelsBuffer(Vector2.zero, self.size, self.buffer)
end

function Canvas.SetPixel(self: Canvas, canvasX: number, canvasY: number, color: number)
    buffer.writeu32(self.buffer, canvasY * self.width * 4 + canvasX * 4, color)
end

function Canvas.Clear(self: Canvas)
    buffer.fill(self.buffer, 0, 0x0, self.width * self.height * 4)
end

function Canvas.Mount(self: Canvas, parent: Instance)
    if not self.instance then
        error("Canvas instance is not created.")
    end
    
    self.instance.Parent = parent
end

function Canvas.Destroy(self: Canvas)
    self.image:Destroy()
    self.instance:Destroy()
end

return Canvas