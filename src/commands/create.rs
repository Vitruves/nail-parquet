use clap::Args;
use datafusion::prelude::*;
use datafusion::logical_expr::{lit, Expr};
use crate::utils::io::read_data;
use crate::utils::output::OutputHandler;
use crate::cli::CommonArgs;
use crate::error::{NailError, NailResult};

#[derive(Args, Clone)]
pub struct CreateArgs {
    #[command(flatten)]
    pub common: CommonArgs,

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
}

pub async fn execute(args: CreateArgs) -> NailResult<()> {
    args.common.log_if_verbose(&format!("Reading data from: {}", args.common.input.display()));

    let df = read_data(&args.common.input).await?;
    let mut result_df = df;

    // Apply row filter if specified
    if let Some(row_expr) = &args.row_filter {
        args.common.log_if_verbose(&format!("Applying row filter: {}", row_expr));
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

        args.common.log_if_verbose(&format!("Creating columns: {:?}", column_map));

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
    let output_handler = OutputHandler::new(&args.common);
    output_handler.handle_output(&result_df, "create").await?;

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
