"""
Styles package for Data Processing Tool

Contains various stylesheet implementations:
- material_design: Modern Material Design 3 styles
"""

from .material_design import (
    get_material_stylesheet,
    get_color,
    MATERIAL_MINIMAL,
    MATERIAL_FULL,
    COLORS,
)

__all__ = [
    'get_material_stylesheet',
    'get_color',
    'MATERIAL_MINIMAL',
    'MATERIAL_FULL',
    'COLORS',
]
