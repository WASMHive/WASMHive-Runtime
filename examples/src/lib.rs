use wasm_bindgen::prelude::*;
use std::num::NonZeroU64;
use wgpu::util::DeviceExt;

// Native-only code (not compiled to WASM)
#[cfg(not(target_arch = "wasm32"))]
pub mod native {
    use serde::{Deserialize, Serialize};

    // Data structures
    #[derive(Clone, Serialize, Deserialize, Debug)]
    pub struct TestData {
        pub numbers: Vec<f32>,
    }

    #[derive(Clone, Serialize, Deserialize, Debug)]
    pub struct TestResult {
        pub value: f32,
    }

    // Chunker function - splits TestData into individual elements
    pub fn chunker(data: &TestData) -> Vec<TestData> {
        data.numbers
            .iter()
            .map(|&num| TestData { numbers: vec![num] })
            .collect()
    }

    // Reducer function - sums all results
    pub fn reducer(results: Vec<TestResult>) -> TestResult {
        let total: f32 = results.iter().map(|r| r.value).sum();
        TestResult { value: total }
    }
}

// Re-export for convenience when not targeting WASM
#[cfg(not(target_arch = "wasm32"))]
pub use native::*;

// WASM functions - these are compiled to WASM and run on workers
#[wasm_bindgen]
pub fn cpu_map(x: f32) -> f32 {
    x
}

#[wasm_bindgen]
pub async fn gpu_map(input: Vec<f32>) -> Vec<f32> {
    use log::info;
    console_log::init_with_level(log::Level::Info).ok(); // ok() to ignore if already initialized

    info!("🎮 gpu_map called with {} elements", input.len());

    if input.is_empty() {
        info!("⚠️ Empty input, returning empty result");
        return vec![];
    }

    info!("🔧 Creating WebGPU instance...");
    let instance = wgpu::Instance::new(&wgpu::InstanceDescriptor::default());

    info!("🔍 Requesting WebGPU adapter...");
    let adapter = match instance
        .request_adapter(&wgpu::RequestAdapterOptions::default())
        .await
    {
        Ok(adapter) => {
            info!("✅ WebGPU adapter created successfully");
            adapter
        }
        Err(e) => {
            info!("❌ Failed to create WebGPU adapter: {:?}", e);
            info!("🔄 Falling back to CPU computation");
            // Fallback to CPU computation
            return input.iter().map(|&x| x * x).collect();
        }
    };

    let downlevel_capabilities = adapter.get_downlevel_capabilities();
    if !downlevel_capabilities
        .flags
        .contains(wgpu::DownlevelFlags::COMPUTE_SHADERS)
    {
        info!("❌ Adapter does not support compute shaders");
        info!("🔄 Falling back to CPU computation");
        return input.iter().map(|&x| x * x).collect();
    }
    info!("✅ Compute shaders supported");

    info!("🔧 Requesting WebGPU device...");
    // Use the adapter's supported limits to avoid compatibility issues
    let adapter_limits = adapter.limits();
    let (device, queue) = match adapter
        .request_device(&wgpu::DeviceDescriptor {
            label: None,
            required_features: wgpu::Features::empty(),
            required_limits: adapter_limits,
            memory_hints: wgpu::MemoryHints::MemoryUsage,
            trace: wgpu::Trace::Off,
        })
        .await
    {
        Ok((device, queue)) => {
            info!("✅ WebGPU device created successfully");
            (device, queue)
        }
        Err(e) => {
            info!("❌ Failed to create device: {:?}", e);
            info!("🔄 Falling back to CPU computation");
            return input.iter().map(|&x| x * x).collect();
        }
    };

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
        entry_point: Some("doubleMe"),
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

    info!("📥 Mapping buffer to read results...");
    // Properly await the mapping
    let (sender, receiver) = futures::channel::oneshot::channel();
    buffer_slice.map_async(wgpu::MapMode::Read, move |v| {
        sender.send(v).unwrap();
    });
    let _ = device.poll(wgpu::PollType::Wait);

    // Await the mapping result
    match receiver.await {
        Ok(Ok(())) => {
            info!("✅ Buffer mapped successfully");
        }
        Ok(Err(e)) => {
            info!("❌ Buffer mapping error: {:?}", e);
            info!("🔄 Falling back to CPU computation");
            return input.iter().map(|&x| x * x).collect();
        }
        Err(e) => {
            info!("❌ Failed to receive mapping result: {:?}", e);
            info!("🔄 Falling back to CPU computation");
            return input.iter().map(|&x| x * x).collect();
        }
    }

    let data = buffer_slice.get_mapped_range();
    let result: Vec<f32> = bytemuck::cast_slice(&data).to_vec();

    info!("🎉 GPU computation completed! Processed {} values", result.len());
    result
}
