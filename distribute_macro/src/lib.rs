use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn, ReturnType};

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

    // WASM generation now happens at runtime, not compile time

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
            // Capture function body as string for WASM generation
            let function_body = stringify!(#fn_block);
            distribute_runtime::run_distributed_impl_with_code(
                #fn_name,
                input,
                #chunker_ident,
                #reducer_ident,
                execution_mode,
                function_body,
                stringify!(#fn_name)
            ).await
        }
    };

    TokenStream::from(expanded)
}

// Removed unused function - WASM generation now happens at runtime
