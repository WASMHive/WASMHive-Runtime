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

// GPU map function - uses WebGPU compute shaders for parallel processing (native only)
#[cfg(not(target_arch = "wasm32"))]
#[wasm_bindgen]
pub async fn gpu_map_native(input: Vec<f32>) -> Vec<f32> {
    console_log::init_with_level(log::Level::Info).expect("Failed to initialize logger");

    if input.is_empty() {
        return vec![];
    }

    let instance = wgpu::Instance::new(wgpu::InstanceDescriptor::default());

    let adapter = instance
        .request_adapter(&wgpu::RequestAdapterOptions::default())
        .await
        .expect("Failed to create adapter");

    let downlevel_capabilities = adapter.get_downlevel_capabilities();
    if !downlevel_capabilities
        .flags
        .contains(wgpu::DownlevelFlags::COMPUTE_SHADERS)
    {
        panic!("Adapter does not support compute shaders");
    }

    let (device, queue) = adapter
        .request_device(
            &wgpu::DeviceDescriptor {
                label: None,
                required_features: wgpu::Features::empty(),
                required_limits: wgpu::Limits::downlevel_defaults(),
                memory_hints: wgpu::MemoryHints::MemoryUsage,
            },
            None,
        )
        .await
        .expect("Failed to create device");

    let module = device.create_shader_module(wgpu::include_wgsl!("shader.wgsl"));

    let input_data_buffer = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
        label: None,
        contents: bytemuck::cast_slice(&input),
        usage: wgpu::BufferUsages::STORAGE,
    });

    let output_data_buffer = device.create_buffer(&wgpu::BufferDescriptor {
        label: None,
        size: input_data_buffer.size(),
        usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
        mapped_at_creation: false,
    });

    let download_buffer = device.create_buffer(&wgpu::BufferDescriptor {
        label: None,
        size: input_data_buffer.size(),
        usage: wgpu::BufferUsages::COPY_DST | wgpu::BufferUsages::MAP_READ,
        mapped_at_creation: false,
    });

    let bind_group_layout = device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
        label: None,
        entries: &[
            wgpu::BindGroupLayoutEntry {
                binding: 0,
                visibility: wgpu::ShaderStages::COMPUTE,
                ty: wgpu::BindingType::Buffer {
                    ty: wgpu::BufferBindingType::Storage { read_only: true },
                    min_binding_size: Some(NonZeroU64::new(4).unwrap()),
                    has_dynamic_offset: false,
                },
                count: None,
            },
            wgpu::BindGroupLayoutEntry {
                binding: 1,
                visibility: wgpu::ShaderStages::COMPUTE,
                ty: wgpu::BindingType::Buffer {
                    ty: wgpu::BufferBindingType::Storage { read_only: false },
                    min_binding_size: Some(NonZeroU64::new(4).unwrap()),
                    has_dynamic_offset: false,
                },
                count: None,
            },
        ],
    });

    let bind_group = device.create_bind_group(&wgpu::BindGroupDescriptor {
        label: None,
        layout: &bind_group_layout,
        entries: &[
            wgpu::BindGroupEntry {
                binding: 0,
                resource: input_data_buffer.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 1,
                resource: output_data_buffer.as_entire_binding(),
            },
        ],
    });

    let pipeline_layout = device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
        label: None,
        bind_group_layouts: &[&bind_group_layout],
        push_constant_ranges: &[],
    });

    let pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
        label: None,
        layout: Some(&pipeline_layout),
        module: &module,
        entry_point: "cubeMe", // Updated to match our shader function
        compilation_options: wgpu::PipelineCompilationOptions::default(),
        cache: None,
    });

    let mut encoder =
        device.create_command_encoder(&wgpu::CommandEncoderDescriptor { label: None });

    let mut compute_pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
        label: None,
        timestamp_writes: None,
    });

    compute_pass.set_pipeline(&pipeline);
    compute_pass.set_bind_group(0, &bind_group, &[]);
    let workgroup_count = input.len().div_ceil(64);
    compute_pass.dispatch_workgroups(workgroup_count as u32, 1, 1);
    drop(compute_pass);

    encoder.copy_buffer_to_buffer(
        &output_data_buffer,
        0,
        &download_buffer,
        0,
        output_data_buffer.size(),
    );

    let command_buffer = encoder.finish();
    queue.submit([command_buffer]);

    let buffer_slice = download_buffer.slice(..);

    // Properly await the mapping
    let (sender, receiver) = futures::channel::oneshot::channel();
    buffer_slice.map_async(wgpu::MapMode::Read, move |v| {
        sender.send(v).unwrap();
    });
    device.poll(wgpu::Maintain::wait()).panic_on_timeout();

    // Await the mapping result
    let _ = receiver.await.expect("Failed to map buffer");

    let data = buffer_slice.get_mapped_range();

    let result: Vec<f32> = bytemuck::cast_slice(&data).to_vec();

    result
}

// Reduce function for combining results
#[wasm_bindgen]
pub fn reduce(x: f32, y: f32) -> f32 {
    x + y
}
