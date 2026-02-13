"""
Create splash screen image for Data Processing Tool
"""

from PIL import Image, ImageDraw, ImageFont
import os

def create_splash_screen():
    """Create a simple splash screen image"""
    
    # Create image with rose gold gradient background
    width, height = 600, 400
    img = Image.new('RGB', (width, height), color='#fef5f1')
    draw = ImageDraw.Draw(img)
    
    # Draw gradient background
    for y in range(height):
        # Gradient from top to bottom
        ratio = y / height
        r = int(254 + (247 - 254) * ratio)
        g = int(245 + (224 - 245) * ratio)
        b = int(241 + (221 - 241) * ratio)
        draw.rectangle([(0, y), (width, y+1)], fill=(r, g, b))
    
    # Draw border
    border_color = '#e8c4c1'
    draw.rectangle([(0, 0), (width-1, height-1)], outline=border_color, width=3)
    
    # Draw center rectangle
    rect_x1, rect_y1 = 50, 80
    rect_x2, rect_y2 = width - 50, height - 80
    draw.rounded_rectangle(
        [(rect_x1, rect_y1), (rect_x2, rect_y2)],
        radius=15,
        fill='#ffffff',
        outline='#c77d8f',
        width=2
    )
    
    # Try to load a font, fallback to default if not available
    try:
        title_font = ImageFont.truetype("arial.ttf", 48)
        subtitle_font = ImageFont.truetype("arial.ttf", 20)
        version_font = ImageFont.truetype("arial.ttf", 16)
    except:
        try:
            title_font = ImageFont.truetype("C:\\Windows\\Fonts\\arial.ttf", 48)
            subtitle_font = ImageFont.truetype("C:\\Windows\\Fonts\\arial.ttf", 20)
            version_font = ImageFont.truetype("C:\\Windows\\Fonts\\arial.ttf", 16)
        except:
            title_font = ImageFont.load_default()
            subtitle_font = ImageFont.load_default()
            version_font = ImageFont.load_default()
    
    # Draw title
    title_text = "Data Processing Tool"
    title_bbox = draw.textbbox((0, 0), title_text, font=title_font)
    title_width = title_bbox[2] - title_bbox[0]
    title_x = (width - title_width) // 2
    title_y = 140
    draw.text((title_x, title_y), title_text, fill='#b76e79', font=title_font)
    
    # Draw subtitle
    subtitle_text = "Loading application..."
    subtitle_bbox = draw.textbbox((0, 0), subtitle_text, font=subtitle_font)
    subtitle_width = subtitle_bbox[2] - subtitle_bbox[0]
    subtitle_x = (width - subtitle_width) // 2
    subtitle_y = 210
    draw.text((subtitle_x, subtitle_y), subtitle_text, fill='#8b5e5d', font=subtitle_font)
    
    # Draw version
    version_text = "Version 2.0.1"
    version_bbox = draw.textbbox((0, 0), version_text, font=version_font)
    version_width = version_bbox[2] - version_bbox[0]
    version_x = (width - version_width) // 2
    version_y = 260
    draw.text((version_x, version_y), version_text, fill='#a8779d', font=version_font)
    
    # Save image
    output_path = os.path.join(os.path.dirname(__file__), 'splash.png')
    img.save(output_path, 'PNG')
    print(f"Splash screen created: {output_path}")
    print(f"  Size: {width}x{height} pixels")
    print(f"  Theme: Rose Gold")
    
    return output_path

if __name__ == '__main__':
    try:
        create_splash_screen()
    except Exception as e:
        print(f"Error creating splash screen: {e}")
        print("\nNote: You need Pillow library installed:")
        print("  pip install Pillow")
