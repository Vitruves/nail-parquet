use clap::Args;
use datafusion::prelude::*;
use datafusion::logical_expr::{lit, Expr};
use std::path::PathBuf;
use crate::error::{NailError, NailResult};
use crate::utils::io::{read_data, write_data};
use crate::utils::format::display_dataframe;
use crate::utils::FileFormat;

#[derive(Args, Clone)]
pub struct CreateArgs {
    #[arg(help = "Input file")]
    pub input: PathBuf,

    #[arg(short = 'c', long = "column", 
          help = "Column creation specs (name=expression), comma-separated.\n\
                  Supported operators:\n\
                  • Arithmetic: +, -, *, / (e.g., 'total=price*quantity')\n\
                  • Comparison: >, < (e.g., 'is_expensive=price>100')\n\
                  • Parentheses: () for grouping (e.g., 'result=(a+b)*c')\n\
                  • Column references: Use existing column names\n\
                  • Constants: Numeric values (integers and floats)\n\
                  Examples:\n\
                  • Simple: 'doubled=value*2'\n\
                  • Complex: 'profit=(revenue-cost)/revenue*100'\n\
                  • Conditional: 'high_value=amount>1000'")]
    pub columns: Option<String>,

    #[arg(short = 'r', long = "row", help = "Row filter expression")]
    pub row_filter: Option<String>,

    #[arg(short, long, help = "Output file (if not specified, prints to console)")]
    pub output: Option<PathBuf>,

    #[arg(short, long, help = "Output format", value_enum)]
    pub format: Option<crate::cli::OutputFormat>,

    #[arg(short, long, help = "Number of parallel jobs")]
    pub jobs: Option<usize>,

    #[arg(short, long, help = "Enable verbose output")]
    pub verbose: bool,
}

pub async fn execute(args: CreateArgs) -> NailResult<()> {
    if args.verbose {
        eprintln!("Reading data from: {}", args.input.display());
    }

    let df = read_data(&args.input).await?;
    let mut result_df = df;

    // Apply row filter if specified
    if let Some(row_expr) = &args.row_filter {
        if args.verbose {
            eprintln!("Applying row filter: {}", row_expr);
        }
        let filter_expr = parse_expression(row_expr, &result_df)?;
        result_df = result_df.filter(filter_expr)?;
    }

    // Parse and apply column creation specs
    if let Some(col_specs) = &args.columns {
        let mut column_map = Vec::new();
        for pair in col_specs.split(',') {
            let parts: Vec<&str> = pair.split('=').collect();
            if parts.len() != 2 {
                return Err(NailError::InvalidArgument(format!("Invalid column spec: {}", pair)));
            }
            let name = parts[0].trim();
            let expr = parts[1].trim();
            column_map.push((name.to_string(), expr.to_string()));
        }

        if args.verbose {
            eprintln!("Creating columns: {:?}", column_map);
        }

        // Validate column names don't already exist
        let existing_columns: Vec<String> = result_df.schema().fields().iter()
            .map(|f| f.name().clone()).collect();
        
        for (name, _) in &column_map {
            if existing_columns.contains(name) {
                return Err(NailError::InvalidArgument(format!("Column '{}' already exists", name)));
            }
        }

        // Build select expressions including new columns
        let mut select_exprs: Vec<Expr> = result_df.schema().fields().iter()
            .map(|f| Expr::Column(datafusion::common::Column::new(None::<String>, f.name())))
            .collect();

        // Add new column expressions
        for (name, expr_str) in &column_map {
            let expr = parse_expression(expr_str, &result_df)?;
            select_exprs.push(expr.alias(name));
        }

        result_df = result_df.select(select_exprs)?;
    }

    // Write or display result
    if let Some(output_path) = &args.output {
        let file_format = match args.format {
            Some(crate::cli::OutputFormat::Json) => Some(FileFormat::Json),
            Some(crate::cli::OutputFormat::Csv) => Some(FileFormat::Csv),
            Some(crate::cli::OutputFormat::Parquet) => Some(FileFormat::Parquet),
            _ => None,
        };
        write_data(&result_df, output_path, file_format.as_ref()).await?;
    } else {
        display_dataframe(&result_df, None, args.format.as_ref()).await?;
    }

    Ok(())
}

fn parse_expression(expr_str: &str, df: &DataFrame) -> NailResult<Expr> {
    let schema = df.schema();
    let existing_columns: Vec<String> = schema.fields().iter()
        .map(|f| f.name().clone()).collect();

    // Simple expression parser
    // This is a basic implementation that supports:
    // - Column references
    // - Constants (numbers)
    // - Basic arithmetic (+, -, *, /)
    // - Parentheses for grouping

    parse_expr_recursive(expr_str.trim(), &existing_columns)
}

fn parse_expr_recursive(expr: &str, columns: &[String]) -> NailResult<Expr> {
    let expr = expr.trim();
    
    // Handle parentheses
    if expr.starts_with('(') && expr.ends_with(')') {
        let inner = &expr[1..expr.len()-1];
        if is_balanced_parens(inner) {
            return parse_expr_recursive(inner, columns);
        }
    }

    // Handle binary operations (lowest precedence first)
    // Comparison operators
    if let Some(pos) = find_operator(expr, &['>', '<']) {
        let (left, op, right) = split_at_operator(expr, pos);
        let left_expr = parse_expr_recursive(left, columns)?;
        let right_expr = parse_expr_recursive(right, columns)?;
        
        return Ok(match op {
            '>' => left_expr.gt(right_expr),
            '<' => left_expr.lt(right_expr),
            _ => unreachable!(),
        });
    }

    // Arithmetic operators
    if let Some(pos) = find_operator(expr, &['+', '-']) {
        let (left, op, right) = split_at_operator(expr, pos);
        let left_expr = parse_expr_recursive(left, columns)?;
        let right_expr = parse_expr_recursive(right, columns)?;
        
        return Ok(match op {
            '+' => left_expr + right_expr,
            '-' => left_expr - right_expr,
            _ => unreachable!(),
        });
    }

    if let Some(pos) = find_operator(expr, &['*', '/']) {
        let (left, op, right) = split_at_operator(expr, pos);
        let left_expr = parse_expr_recursive(left, columns)?;
        let right_expr = parse_expr_recursive(right, columns)?;
        
        return Ok(match op {
            '*' => left_expr * right_expr,
            '/' => left_expr / right_expr,
            _ => unreachable!(),
        });
    }

    // Handle functions
    if expr.contains('(') && expr.ends_with(')') {
        if let Some(func_start) = expr.find('(') {
            let func_name = &expr[..func_start];
            let _args_str = &expr[func_start+1..expr.len()-1];
            
            // For now, return an error for unsupported functions
            return Err(NailError::InvalidArgument(format!("Function '{}' is not yet supported", func_name)));
        }
    }

    // Handle column references
    if columns.contains(&expr.to_string()) {
        return Ok(Expr::Column(datafusion::common::Column::new(None::<String>, expr)));
    }

    // Handle numeric constants
    if let Ok(num) = expr.parse::<f64>() {
        return Ok(lit(num));
    }

    // Handle integer constants
    if let Ok(num) = expr.parse::<i64>() {
        return Ok(lit(num));
    }

    Err(NailError::InvalidArgument(format!("Invalid expression: {}", expr)))
}

fn is_balanced_parens(s: &str) -> bool {
    let mut count = 0;
    for c in s.chars() {
        match c {
            '(' => count += 1,
            ')' => {
                count -= 1;
                if count < 0 {
                    return false;
                }
            }
            _ => {}
        }
    }
    count == 0
}

fn find_operator(expr: &str, ops: &[char]) -> Option<usize> {
    let mut paren_count = 0;
    let chars: Vec<char> = expr.chars().collect();
    
    // Search from right to left for right associativity
    for i in (0..chars.len()).rev() {
        match chars[i] {
            '(' => paren_count += 1,
            ')' => paren_count -= 1,
            c if paren_count == 0 && ops.contains(&c) => return Some(i),
            _ => {}
        }
    }
    None
}

fn split_at_operator(expr: &str, pos: usize) -> (&str, char, &str) {
    let chars: Vec<char> = expr.chars().collect();
    let left = &expr[..pos];
    let op = chars[pos];
    let right = &expr[pos+1..];
    (left.trim(), op, right.trim())
}
