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
    let adapter = instance
        .request_adapter(&wgpu::RequestAdapterOptions::default())
        .await
        .expect("Failed to create WebGPU adapter");
    info!("✅ WebGPU adapter created successfully");

    let downlevel_capabilities = adapter.get_downlevel_capabilities();
    assert!(
        downlevel_capabilities
            .flags
            .contains(wgpu::DownlevelFlags::COMPUTE_SHADERS),
        "Adapter does not support compute shaders"
    );
    info!("✅ Compute shaders supported");

    info!("🔧 Requesting WebGPU device...");
    // Use the adapter's supported limits to avoid compatibility issues
    let adapter_limits = adapter.limits();
    let (device, queue) = adapter
        .request_device(&wgpu::DeviceDescriptor {
            label: None,
            required_features: wgpu::Features::empty(),
            required_limits: adapter_limits,
            memory_hints: wgpu::MemoryHints::MemoryUsage,
            trace: wgpu::Trace::Off,
        })
        .await
        .expect("Failed to create WebGPU device");
    info!("✅ WebGPU device created successfully");

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
    receiver
        .await
        .expect("Failed to receive mapping result")
        .expect("Buffer mapping error");
    info!("✅ Buffer mapped successfully");

    let data = buffer_slice.get_mapped_range();
    let result: Vec<f32> = bytemuck::cast_slice(&data).to_vec();

    info!("🎉 GPU computation completed! Processed {} values", result.len());
    result
}

// Byte-processing WASM function: convert an RGBA frame to grayscale in-place
#[wasm_bindgen]
pub fn grayscale_frame_rgba(mut input: Vec<u8>, _meta: JsValue) -> Vec<u8> {
    let len = input.len();
    let mut i = 0usize;
    while i + 3 < len {
        let r = input[i] as u16;
        let g = input[i + 1] as u16;
        let b = input[i + 2] as u16;
        // Luma approximation
        let gray = ((299 * r + 587 * g + 114 * b) / 1000) as u8;
        input[i] = gray;
        input[i + 1] = gray;
        input[i + 2] = gray;
        // alpha unchanged at i+3
        i += 4;
    }
    input
}

// WASM function to fetch multiple URLs and extract their titles
#[wasm_bindgen]
pub async fn fetch_url_title(url_bytes: Vec<u8>, _meta: JsValue) -> Vec<u8> {
    use wasm_bindgen_futures::JsFuture;
    use web_sys::{Request, RequestInit, RequestMode, Response};
    use log::info;
    console_log::init_with_level(log::Level::Info).ok();

    // Parse JSON array of URLs
    let urls_json = match String::from_utf8(url_bytes) {
        Ok(s) => s,
        Err(_) => {
            info!("⚠️ Failed to decode URL bytes");
            return b"[]".to_vec();
        }
    };

    let urls: Vec<String> = match serde_json::from_str(&urls_json) {
        Ok(urls) => urls,
        Err(e) => {
            info!("⚠️ Failed to parse URLs JSON: {:?}", e);
            return b"[]".to_vec();
        }
    };

    let url_count = urls.len();
    info!("🌐 Fetching batch of {} URLs", url_count);

    // Fetch all URLs in parallel using futures
    #[derive(serde::Serialize)]
    struct UrlTitleResult {
        url: String,
        title: String,
    }
    
    // Create futures for all URL fetches - fetch in parallel
    use futures::future;
    let fetch_futures: Vec<_> = urls.iter().enumerate().map(|(idx, url_str)| {
        let url = url_str.clone();
        async move {
            info!("   🔄 [{}/{}] Starting fetch: {}", idx + 1, url_count, url);
            let title = match fetch_single_url(&url).await {
                Ok(t) => {
                    info!("   ✅ [{}/{}] Success: {} -> {}", idx + 1, url_count, url, t);
                    t
                }
                Err(e) => {
                    info!("   ⚠️ [{}/{}] Error: {} -> {}", idx + 1, url_count, url, e);
                    format!("ERROR: {}", e)
                }
            };
            UrlTitleResult {
                url,
                title,
            }
        }
    }).collect();
    
    info!("   ⏳ Awaiting {} parallel fetches...", fetch_futures.len());
    
    // Execute all fetches in parallel
    let results: Vec<UrlTitleResult> = future::join_all(fetch_futures).await;

    // Convert results to JSON array
    let results_json = serde_json::to_string(&results).unwrap_or_else(|_| "[]".to_string());
    info!("✅ Completed batch: {} URLs processed", results.len());
    
    results_json.into_bytes()
}

// Helper function to URL encode (simple implementation)
fn url_encode(s: &str) -> String {
    let mut encoded = String::new();
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            _ => {
                encoded.push('%');
                encoded.push_str(&format!("{:02X}", byte));
            }
        }
    }
    encoded
}

// Helper function to fetch a single URL and extract its title
async fn fetch_single_url(url_str: &str) -> Result<String, String> {
    use wasm_bindgen_futures::JsFuture;
    use web_sys::{Request, RequestInit, RequestMode, Response};
    use log::info;

    // Use local CORS proxy to bypass browser CORS restrictions
    // Make sure proxy-server.js is running on port 3001
    let encoded_url = url_encode(url_str);
    let fetch_url = format!("http://localhost:3001/proxy?url={}", encoded_url);
    
    // Create a fetch request
    let mut opts = RequestInit::new();
    opts.method("GET");
    opts.mode(RequestMode::Cors);

    let request = Request::new_with_str_and_init(&fetch_url, &opts)
        .map_err(|e| format!("Failed to create request: {:?}", e))?;

    // Fetch the URL
    let window = web_sys::window().expect("no global `window` exists");
    let fetch_promise = window.fetch_with_request(&request);
    
    // Handle fetch errors (likely CORS)
    let response = match JsFuture::from(fetch_promise).await {
        Ok(resp) => resp,
        Err(e) => {
            // Check if it's a CORS error
            let error_msg = format!("{:?}", e);
            if error_msg.contains("Failed to fetch") || error_msg.contains("CORS") {
                return Err(format!("CORS blocked: Browser security prevents fetching this URL. Consider using a CORS proxy or server-side fetching."));
            }
            return Err(format!("Fetch failed: {:?}", e));
        }
    };
    
    let resp: Response = response.dyn_into().unwrap();
    if !resp.ok() {
        return Err(format!("HTTP {}", resp.status()));
    }

    // Get the response text
    let text_promise = resp.text().unwrap();
    let text = JsFuture::from(text_promise).await
        .map_err(|e| format!("Failed to read response: {:?}", e))?;
    
    let text_str: String = text.as_string().unwrap_or_default();

    // Extract title from HTML
    let title = extract_title_from_html(&text_str);
    Ok(title)
}

// Helper function to extract title from HTML
fn extract_title_from_html(html: &str) -> String {
    // Look for <title> tag (case-insensitive)
    let html_lower = html.to_lowercase();
    
    // Find <title> tag
    if let Some(title_start) = html_lower.find("<title>") {
        let title_content_start = title_start + 7; // length of "<title>"
        if let Some(title_end) = html[title_content_start..].find("</title>") {
            let title = html[title_content_start..title_content_start + title_end].trim();
            // Decode HTML entities if needed (basic handling)
            let title = title
                .replace("&amp;", "&")
                .replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&quot;", "\"")
                .replace("&#39;", "'")
                .replace("&nbsp;", " ");
            return title.to_string();
        }
    }
    
    // Fallback: try to find title in meta tags
    if let Some(meta_start) = html_lower.find("property=\"og:title\"") {
        if let Some(content_start) = html[meta_start..].find("content=\"") {
            let content_start = meta_start + content_start + 9; // length of "content=\""
            if let Some(content_end) = html[content_start..].find("\"") {
                let title = html[content_start..content_start + content_end].trim();
                return title.to_string();
            }
        }
    }
    
    "NO TITLE FOUND".to_string()
}
