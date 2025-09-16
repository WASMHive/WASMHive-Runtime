use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn, ReturnType};
use std::fs;
use std::path::Path;

#[proc_macro_attribute]
pub fn distribute(args: TokenStream, input: TokenStream) -> TokenStream {
    let args_str = args.to_string();
    let arg_parts: Vec<&str> = args_str.split(',').map(|s| s.trim()).collect();

    if arg_parts.len() != 2 {
        panic!("distribute macro expects exactly 2 arguments: chunker and reducer function names");
    }

    let chunker_name = arg_parts[0];
    let reducer_name = arg_parts[1];

    let input_fn = parse_macro_input!(input as ItemFn);

    // Extract function details
    let fn_name = &input_fn.sig.ident;
    let fn_vis = &input_fn.vis;
    let fn_inputs = &input_fn.sig.inputs;
    let fn_output = &input_fn.sig.output;
    let fn_block = &input_fn.block;

    // Extract input and output types
    let input_type = if let Some(syn::FnArg::Typed(pat_type)) = fn_inputs.first() {
        &pat_type.ty
    } else {
        panic!("Function must have at least one parameter");
    };

    let output_type = match fn_output {
        ReturnType::Type(_, ty) => ty.as_ref(),
        ReturnType::Default => {
            panic!("Function must have a return type");
        }
    };

    // Generate WASM module with the original function and WASM bindings
    generate_wasm_module(&fn_name.to_string(), input_type, output_type, &input_fn);

    // Generate the distributed function name based on the original function name
    let distributed_fn_name = syn::Ident::new(&format!("{}_run_distributed", fn_name), proc_macro2::Span::call_site());

    let chunker_ident = syn::Ident::new(chunker_name, proc_macro2::Span::call_site());
    let reducer_ident = syn::Ident::new(reducer_name, proc_macro2::Span::call_site());

    // Generate the code
    let expanded = quote! {
        // Keep the original function unchanged
        #fn_vis fn #fn_name(#fn_inputs) #fn_output {
            #fn_block
        }

        // Generate the run_distributed function
        #fn_vis async fn #distributed_fn_name(
            input: #input_type,
            execution_mode: distribute_runtime::ExecutionMode,
        ) -> #output_type {
            distribute_runtime::run_distributed_impl(
                #fn_name,
                input,
                #chunker_ident,
                #reducer_ident,
                execution_mode
            ).await
        }
    };

    TokenStream::from(expanded)
}

fn generate_wasm_module(fn_name: &str, input_type: &syn::Type, _output_type: &syn::Type, input_fn: &ItemFn) {
    let wasm_content = format!(r#"
use wasm_bindgen::prelude::*;
use serde::{{Serialize, Deserialize}};

// GPU compute functions (from your provided code)
use std::num::NonZeroU64;
use wgpu::util::DeviceExt;

#[wasm_bindgen]
pub fn map(x: i32) -> i32 {{
    x * 2
}}

#[wasm_bindgen]
pub fn reduce(x: f32, y: f32) -> f32 {{
    x + y
}}

#[wasm_bindgen]
pub async fn gpu_map(input: Vec<f32>) -> Vec<f32> {{
    console_log::init_with_level(log::Level::Info).expect("Failed to initialize logger");

    if input.is_empty() {{
        return vec![];
    }}

    let instance = wgpu::Instance::new(&wgpu::InstanceDescriptor::default());

    let adapter = instance
        .request_adapter(&wgpu::RequestAdapterOptions::default())
        .await
        .expect("Failed to create adapter");

    let downlevel_capabilities = adapter.get_downlevel_capabilities();
    if !downlevel_capabilities
        .flags
        .contains(wgpu::DownlevelFlags::COMPUTE_SHADERS)
    {{
        panic!("Adapter does not support compute shaders");
    }}

    let (device, queue) = adapter
        .request_device(&wgpu::DeviceDescriptor {{
            label: None,
            required_features: wgpu::Features::empty(),
            required_limits: wgpu::Limits::downlevel_defaults(),
            memory_hints: wgpu::MemoryHints::MemoryUsage,
            trace: wgpu::Trace::Off,
        }})
        .await
        .expect("Failed to create device");

    let module = device.create_shader_module(wgpu::include_wgsl!("shader.wgsl"));

    let input_data_buffer = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {{
        label: None,
        contents: bytemuck::cast_slice(&input),
        usage: wgpu::BufferUsages::STORAGE,
    }});

    let output_data_buffer = device.create_buffer(&wgpu::BufferDescriptor {{
        label: None,
        size: input_data_buffer.size(),
        usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
        mapped_at_creation: false,
    }});

    let download_buffer = device.create_buffer(&wgpu::BufferDescriptor {{
        label: None,
        size: input_data_buffer.size(),
        usage: wgpu::BufferUsages::COPY_DST | wgpu::BufferUsages::MAP_READ,
        mapped_at_creation: false,
    }});

    let bind_group_layout = device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {{
        label: None,
        entries: &[
            wgpu::BindGroupLayoutEntry {{
                binding: 0,
                visibility: wgpu::ShaderStages::COMPUTE,
                ty: wgpu::BindingType::Buffer {{
                    ty: wgpu::BufferBindingType::Storage {{ read_only: true }},
                    min_binding_size: Some(NonZeroU64::new(4).unwrap()),
                    has_dynamic_offset: false,
                }},
                count: None,
            }},
            wgpu::BindGroupLayoutEntry {{
                binding: 1,
                visibility: wgpu::ShaderStages::COMPUTE,
                ty: wgpu::BindingType::Buffer {{
                    ty: wgpu::BufferBindingType::Storage {{ read_only: false }},
                    min_binding_size: Some(NonZeroU64::new(4).unwrap()),
                    has_dynamic_offset: false,
                }},
                count: None,
            }},
        ],
    }});

    let bind_group = device.create_bind_group(&wgpu::BindGroupDescriptor {{
        label: None,
        layout: &bind_group_layout,
        entries: &[
            wgpu::BindGroupEntry {{
                binding: 0,
                resource: input_data_buffer.as_entire_binding(),
            }},
            wgpu::BindGroupEntry {{
                binding: 1,
                resource: output_data_buffer.as_entire_binding(),
            }},
        ],
    }});

    let pipeline_layout = device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {{
        label: None,
        bind_group_layouts: &[&bind_group_layout],
        push_constant_ranges: &[],
    }});

    let pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {{
        label: None,
        layout: Some(&pipeline_layout),
        module: &module,
        entry_point: Some("doubleMe"),
        compilation_options: wgpu::PipelineCompilationOptions::default(),
        cache: None,
    }});

    let mut encoder =
        device.create_command_encoder(&wgpu::CommandEncoderDescriptor {{ label: None }});

    let mut compute_pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {{
        label: None,
        timestamp_writes: None,
    }});

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

    let (sender, receiver) = futures::channel::oneshot::channel();
    buffer_slice.map_async(wgpu::MapMode::Read, move |v| {{
        sender.send(v).unwrap();
    }});
    let _ = device.poll(wgpu::PollType::Wait);

    let _ = receiver.await.expect("Failed to map buffer");

    let data = buffer_slice.get_mapped_range();
    let result: Vec<f32> = bytemuck::cast_slice(&data).to_vec();

    result
}}

// User's distributed function compiled for WASM
{function_body}

#[wasm_bindgen]
pub fn {fn_name}_wasm(input: &str) -> String {{
    let parsed_input: {input_type} = serde_json::from_str(input).unwrap();
    let result = {fn_name}(parsed_input);
    serde_json::to_string(&result).unwrap()
}}
"#,
        function_body = quote!(#input_fn).to_string(),
        fn_name = fn_name,
        input_type = quote!(#input_type).to_string()
    );

    // Write the WASM module to a temporary location
    let wasm_dir = Path::new("target/wasm");
    if !wasm_dir.exists() {
        fs::create_dir_all(wasm_dir).unwrap();
    }

    let wasm_file = wasm_dir.join(format!("{}_wasm.rs", fn_name));
    fs::write(wasm_file, wasm_content).unwrap();
}
