use wasm_bindgen::prelude::*;

#[cfg(not(target_arch = "wasm32"))]
use std::num::NonZeroU64;
#[cfg(not(target_arch = "wasm32"))]
use wgpu::util::DeviceExt;

// Native build utilities removed - now handled directly by the runtime

// CPU map function - cubes each number
#[wasm_bindgen]
pub fn cpu_map(x: f32) -> f32 {
    x
}

// GPU map function - same computation as CPU for consistency
#[wasm_bindgen]
pub fn gpu_map(x: f32) -> f32 {
    x * x
}

#[wasm_bindgen]
pub fn cpu1_map(x: f32) -> f32 {
    x * x * x
}
