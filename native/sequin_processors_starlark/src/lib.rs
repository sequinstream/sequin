use rustler::{Binary, Env, Error, NifResult, OwnedBinary, Term};
use rustler::Encoder;
use starlark::environment::{GlobalsBuilder, Module};
use starlark::eval::{Evaluator, ReturnFileLoader};
use starlark::syntax::{AstModule, Dialect};
use starlark::values::Heap;
use starlark::environment::LibraryExtension;
use std::collections::HashMap;
use std::cell::RefCell;

mod atoms {
    rustler::atoms! {
        ok,
        error,
    }
}

#[rustler::nif]
fn add(a: i64, b: i64) -> i64 {
    a + b
}

// Define custom Starlark functions
#[starlark::starlark_module]
fn starlark_custom_functions(builder: &mut GlobalsBuilder) {
    // A simple string manipulation function
    fn concat(a: &str, b: &str) -> anyhow::Result<String> {
        Ok(format!("{}{}", a, b))
    }

    // A function to get system time
    fn timestamp() -> anyhow::Result<i64> {
        use std::time::{SystemTime, UNIX_EPOCH};
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| anyhow::anyhow!("Time error: {}", e))?;
        Ok(time.as_secs() as i64)
    }

    // A function to check if a number is even
    fn is_even(x: i64) -> anyhow::Result<bool> {
        Ok(x % 2 == 0)
    }
    
    // Convert ISO 8601 timestamp to Unix microseconds
    fn ts_unix_micro(timestamp_str: &str) -> anyhow::Result<i64> {
        use chrono::prelude::*;
        
        let dt = DateTime::parse_from_rfc3339(timestamp_str)
            .map_err(|e| anyhow::anyhow!("Invalid timestamp format: {}", e))?;
            
        // Convert to microseconds (seconds * 1_000_000 + microseconds)
        let unix_micro = dt.timestamp() * 1_000_000 + (dt.timestamp_subsec_micros() as i64);
        
        Ok(unix_micro)
    }
}

// Starlark context struct which groups interpreter state
struct Ctx {
    module: *mut Module,
    globals: starlark::environment::Globals,
}

// Ensure the struct can be safely shared between threads
unsafe impl Send for Ctx {}
unsafe impl Sync for Ctx {}

impl Ctx {
    fn module(&self) -> &Module {
        unsafe { &*self.module }
    }
    
    fn module_mut(&mut self) -> &mut Module {
        unsafe { &mut *self.module }
    }
}

// Thread-local storage for Starlark context
thread_local! {
    static CTX: RefCell<Option<Ctx>> = RefCell::new(None);
}

// Initialize the Starlark context
fn init_ctx() -> Result<(), anyhow::Error> {
    let globals = GlobalsBuilder::extended_by(&[LibraryExtension::Json])
        .with(starlark_custom_functions)
        .build();
    
    // Create a module that will persist
    let module = Box::new(Module::new());
    let module_ptr = Box::into_raw(module);
    
    let ctx = Ctx {
        module: module_ptr,
        globals,
    };
    
    CTX.with(|cell| {
        *cell.borrow_mut() = Some(ctx);
    });
    
    Ok(())
}

// Get a reference to the current context
fn get_ctx() -> Result<Ctx, anyhow::Error> {
    let mut result = None;
    
    CTX.with(|cell| {
        if let Some(ctx) = &*cell.borrow() {
            // Create a copy of the context with the same pointers
            result = Some(Ctx {
                module: ctx.module,
                globals: ctx.globals.clone(),
            });
        }
    });
    
    result.ok_or_else(|| anyhow::anyhow!("Starlark context not initialized"))
}

#[rustler::nif]
fn load_code_nif<'a>(env: Env<'a>, code: Binary) -> NifResult<Term<'a>> {
    let code_str = std::str::from_utf8(&code)
        .map_err(|_| Error::Term(Box::new("Invalid UTF-8 in input")))?;
    
    // Initialize context if it doesn't exist
    CTX.with(|cell| {
        if cell.borrow().is_none() {
            init_ctx().map_err(|e| Error::Term(Box::new(format!("Init error: {}", e))))?;
        }
        Ok::<_, Error>(())
    })?;
    
    // Parse the code
    let ast = AstModule::parse("input.star", code_str.to_owned(), &Dialect::Standard)
        .map_err(|e| Error::Term(Box::new(format!("Parse error: {}", e))))?;
    
    // Use a block to ensure all borrows are dropped before returning
    {
        // Get mutable access to the context
        let ctx_result = CTX.with(|cell| -> Result<(), Error> {
            let mut ctx_ref = cell.borrow_mut();
            let ctx = ctx_ref.as_mut().unwrap();
            
            // Clone the globals to avoid borrowing issues
            let globals = ctx.globals.clone();
            let module = ctx.module_mut();
            
            // Execute the code
            let mut eval = Evaluator::new(module);
            match eval.eval_module(ast, &globals) {
                Ok(_) => Ok(()), // Discard the result, we only care about side effects
                Err(e) => Err(Error::Term(Box::new(format!("Evaluation error: {}", e))))
            }
        });
        
        // Check for evaluation errors
        ctx_result?;
    }
    
    // Return an empty successful result
    Ok(atoms::ok().encode(env))
}

// Call a specific function from the loaded Starlark module
#[rustler::nif]
fn call_function_nif<'a>(env: Env<'a>, function_name: Binary, args: rustler::ListIterator) -> NifResult<Term<'a>> {
    // Debug output to console
    println!("call_function_nif called with function:");

    let function_name_str = std::str::from_utf8(&function_name)
        .map_err(|_| Error::Term(Box::new("Invalid UTF-8 in function name")))?;
    
    // Get the context
    let ctx = get_ctx()
        .map_err(|e| Error::Term(Box::new(format!("Context error: {}", e))))?;
    
    // Check if the function exists
    let func = ctx.module().get(function_name_str)
        .ok_or_else(|| Error::Term(Box::new(format!("Function '{}' not found", function_name_str))))?;

    // Create a heap for Starlark values
    let heap = Heap::new();

    // Convert Elixir args to Starlark values
    let mut starlark_args = Vec::new();
    for arg in args {
        if let Ok(int_val) = arg.decode::<i64>() {
            println!("arg was an integer: {}", int_val);
            starlark_args.push(heap.alloc(int_val));
        } else if let Ok(float_val) = arg.decode::<f64>() {
            println!("arg was a float: {}", float_val);
            starlark_args.push(heap.alloc(float_val));
        } else if let Ok(string_val) = arg.decode::<Binary>() {
            println!("arg was a string");
            let s = std::str::from_utf8(&string_val)
                .map_err(|_| Error::Term(Box::new("Invalid UTF-8 in argument")))?;
            starlark_args.push(heap.alloc(s));
        } else {
            println!("arg was an unsupported type");
            println!("Debug - argument type: {:?}", arg);
            println!("Debug - term type info: {:?}", arg.get_type());

            return Err(Error::Term(Box::new("Unsupported argument type")));
        }
    }

    // Call the function - create a new evaluator for each call to avoid lifetime issues
    let module = ctx.module();
    let mut eval = Evaluator::new(module);
    println!("Debug - starlark_args before evaluation: {:?}", starlark_args);
    let result = eval.eval_function(func, &starlark_args, &[])
        .map_err(move |e| Error::Term(Box::new(format!("Function call error: {}", e))))?;

    // Convert result to JSON and return to Elixir
    let json_result = result.to_json()
        .map_err(|e| Error::Term(Box::new(format!("JSON conversion error: {}", e))))?;

    // Create a new binary to return to Elixir
    let mut bin = OwnedBinary::new(json_result.len())
        .ok_or_else(|| Error::Term(Box::new("Could not allocate binary")))?;
    
    bin.as_mut_slice().copy_from_slice(json_result.as_bytes());
    
    Ok(Binary::from_owned(bin, env).to_term(env))
}

// Module evaluation (original function kept for backward compatibility)
fn eval_module_with_code(
    code: &str, 
    module_name: &str, 
    modules: &HashMap<String, String>
) -> anyhow::Result<String> {
    // Create globals with custom functions
    let globals = GlobalsBuilder::standard()
        .with(starlark_custom_functions)
        .build();

    // Parse the AST
    let ast = AstModule::parse(module_name, code.to_owned(), &Dialect::Standard)
        .map_err(|e| anyhow::anyhow!("Parse error: {}", e))?;

    // Create a module
    let module = Module::new();
    
    // Evaluate with or without modules
    let result = if modules.is_empty() {
        // Simple evaluation without imports
        let mut eval = Evaluator::new(&module);
        eval.eval_module(ast, &globals)
            .map_err(|e| anyhow::anyhow!("Evaluation error: {}", e))
    } else {
        // Pre-process additional modules
        let mut frozen_modules = HashMap::new();
        
        for (name, contents) in modules {
            let module_ast = AstModule::parse(name, contents.to_owned(), &Dialect::Standard)
                .map_err(|e| anyhow::anyhow!("Module parse error for {}: {}", name, e))?;
            let import_module = Module::new();
            {
                let mut eval = Evaluator::new(&import_module);
                eval.eval_module(module_ast, &globals)
                    .map_err(|e| anyhow::anyhow!("Module eval error for {}: {}", name, e))?;
            }
            frozen_modules.insert(name.clone(), import_module.freeze()
                .map_err(|e| anyhow::anyhow!("Module freeze error for {}: {:?}", name, e))?);
        }
        
        // We need to keep modules_map and loader alive for the whole evaluation
        // so we restructure the code to avoid lifetime issues
        let modules_map: HashMap<&str, _> = frozen_modules
            .iter()
            .map(|(k, v)| (k.as_str(), v))
            .collect();
        
        let mut loader = ReturnFileLoader {
            modules: &modules_map,
        };
        
        let mut eval = Evaluator::new(&module);
        eval.set_loader(&mut loader);
        
        // Run the evaluation
        eval.eval_module(ast, &globals)
            .map_err(|e| anyhow::anyhow!("Evaluation error: {}", e))
    }?;
    
    // Convert to JSON and return
    result.to_json()
        .map_err(|e| anyhow::anyhow!("JSON conversion error: {}", e))
}

#[rustler::nif]
fn eval_starlark<'a>(env: Env<'a>, code: Binary) -> NifResult<Term<'a>> {
    let code_str = std::str::from_utf8(&code)
        .map_err(|_| Error::Term(Box::new("Invalid UTF-8 in input")))?;
    
    // Evaluate the code with an empty modules map
    let empty_modules = HashMap::new();
    let json_result = eval_module_with_code(code_str, "input.star", &empty_modules)
        .map_err(|e| Error::Term(Box::new(format!("Evaluation error: {}", e))))?;
    
    // Create a new binary to return to Elixir
    let mut bin = OwnedBinary::new(json_result.len())
        .ok_or_else(|| Error::Term(Box::new("Could not allocate binary")))?;
    
    bin.as_mut_slice().copy_from_slice(json_result.as_bytes());
    
    Ok(Binary::from_owned(bin, env).to_term(env))
}

#[rustler::nif]
fn eval_starlark_with_modules<'a>(
    env: Env<'a>,
    code: Binary,
    module_name: Binary,
    modules_map: rustler::MapIterator,
) -> NifResult<Term<'a>> {
    let code_str = std::str::from_utf8(&code)
        .map_err(|_| Error::Term(Box::new("Invalid UTF-8 in input")))?;
    
    let module_name_str = std::str::from_utf8(&module_name)
        .map_err(|_| Error::Term(Box::new("Invalid UTF-8 in module name")))?;

    // Convert modules map to HashMap
    let mut modules = HashMap::new();
    for (key, value) in modules_map {
        let key_bin: Binary = key.decode()
            .map_err(|_| Error::Term(Box::new("Module key must be a binary")))?;
        let value_bin: Binary = value.decode()
            .map_err(|_| Error::Term(Box::new("Module value must be a binary")))?;

        let key_str = std::str::from_utf8(&key_bin)
            .map_err(|_| Error::Term(Box::new("Invalid UTF-8 in module key")))?;
        let value_str = std::str::from_utf8(&value_bin)
            .map_err(|_| Error::Term(Box::new("Invalid UTF-8 in module value")))?;

        modules.insert(key_str.to_owned(), value_str.to_owned());
    }

    // Evaluate the code with the modules
    let json_result = eval_module_with_code(code_str, module_name_str, &modules)
        .map_err(|e| Error::Term(Box::new(format!("Evaluation error: {}", e))))?;
    
    // Create a new binary to return to Elixir
    let mut bin = OwnedBinary::new(json_result.len())
        .ok_or_else(|| Error::Term(Box::new("Could not allocate binary")))?;
    
    bin.as_mut_slice().copy_from_slice(json_result.as_bytes());
    
    Ok(Binary::from_owned(bin, env).to_term(env))
}

rustler::init!("Elixir.Sequin.Processors.Starlark");
