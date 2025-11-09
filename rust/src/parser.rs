// Python AST parser using RustPython's parser
//
// This module parses Python source code and extracts code blocks
// (functions, classes, modules) with their checksums.

use anyhow::{Context, Result};
use crc32fast::Hasher;
use pyo3::prelude::*;
use rustpython_parser::{ast, Parse};

use crate::types::Block;

/// Parse a Python module and extract all code blocks
///
/// # Arguments
/// * `source` - Python source code as a string
///
/// # Returns
/// * `PyResult<Vec<Block>>` - List of blocks found in the source
///
/// # Example
/// ```python
/// blocks = parse_module("def foo(): pass")
/// assert len(blocks) == 2  # module + function
/// ```
#[pyfunction]
pub fn parse_module(source: &str) -> PyResult<Vec<Block>> {
    let blocks = parse_module_internal(source).map_err(|e| {
        pyo3::exceptions::PySyntaxError::new_err(format!("Failed to parse Python code: {}", e))
    })?;

    Ok(blocks)
}

/// Internal implementation that returns anyhow::Result
fn parse_module_internal(source: &str) -> Result<Vec<Block>> {
    // Parse the source code with RustPython's parser
    let parsed = ast::Suite::parse(source, "<string>")
        .map_err(|e| anyhow::anyhow!("Parse error: {}", e))?;

    let mut blocks = Vec::new();

    // Add module-level block (entire file)
    let module_checksum = calculate_checksum(source);
    let line_count = source.lines().count();
    blocks.push(Block {
        start_line: 1,
        end_line: line_count.max(1),
        checksum: module_checksum,
        name: "<module>".to_string(),
        block_type: "module".to_string(),
    });

    // Extract blocks from AST
    extract_blocks_from_statements(&parsed, source, &mut blocks)?;

    Ok(blocks)
}

/// Recursively extract blocks from a list of statements
fn extract_blocks_from_statements(
    statements: &[ast::Stmt],
    source: &str,
    blocks: &mut Vec<Block>,
) -> Result<()> {
    for stmt in statements {
        extract_block_from_statement(stmt, source, blocks)?;
    }
    Ok(())
}

/// Extract a block from a single statement
fn extract_block_from_statement(stmt: &ast::Stmt, source: &str, blocks: &mut Vec<Block>) -> Result<()> {
    match &stmt.node {
        ast::StmtKind::FunctionDef {
            name,
            body,
            ..
        } => {
            let (start, _) = get_location_range(&stmt.location, &None);
            // For now, extract just the function signature line
            // TODO: Improve to extract entire function body
            let block_source = extract_source_lines(source, start, start)?;
            let checksum = calculate_checksum(&block_source);

            blocks.push(Block {
                start_line: start,
                end_line: start,  // Will be improved later with full body extraction
                checksum,
                name: name.to_string(),
                block_type: "function".to_string(),
            });

            // Extract nested blocks
            extract_blocks_from_statements(body, source, blocks)?;
        }
        ast::StmtKind::AsyncFunctionDef {
            name,
            body,
            ..
        } => {
            let (start, _) = get_location_range(&stmt.location, &None);
            let block_source = extract_source_lines(source, start, start)?;
            let checksum = calculate_checksum(&block_source);

            blocks.push(Block {
                start_line: start,
                end_line: start,  // Will be improved later
                checksum,
                name: name.to_string(),
                block_type: "async_function".to_string(),
            });

            extract_blocks_from_statements(body, source, blocks)?;
        }
        ast::StmtKind::ClassDef {
            name,
            body,
            ..
        } => {
            let (start, _) = get_location_range(&stmt.location, &None);
            let block_source = extract_source_lines(source, start, start)?;
            let checksum = calculate_checksum(&block_source);

            blocks.push(Block {
                start_line: start,
                end_line: start,  // Will be improved later
                checksum,
                name: name.to_string(),
                block_type: "class".to_string(),
            });

            extract_blocks_from_statements(body, source, blocks)?;
        }
        // Handle other statement types that may contain nested blocks
        ast::StmtKind::If { body, orelse, .. } => {
            extract_blocks_from_statements(body, source, blocks)?;
            extract_blocks_from_statements(orelse, source, blocks)?;
        }
        ast::StmtKind::For { body, orelse, .. } => {
            extract_blocks_from_statements(body, source, blocks)?;
            extract_blocks_from_statements(orelse, source, blocks)?;
        }
        ast::StmtKind::While { body, orelse, .. } => {
            extract_blocks_from_statements(body, source, blocks)?;
            extract_blocks_from_statements(orelse, source, blocks)?;
        }
        ast::StmtKind::With { body, .. } => {
            extract_blocks_from_statements(body, source, blocks)?;
        }
        ast::StmtKind::Try { body, handlers, orelse, finalbody, .. } => {
            extract_blocks_from_statements(body, source, blocks)?;
            for handler in handlers {
                extract_blocks_from_statements(&handler.node.body, source, blocks)?;
            }
            extract_blocks_from_statements(orelse, source, blocks)?;
            extract_blocks_from_statements(finalbody, source, blocks)?;
        }
        _ => {}
    }
    Ok(())
}

/// Get line range from location info
fn get_location_range(
    start: &ast::Location,
    _end: &Option<ast::Location>,
) -> (usize, usize) {
    let start_line = start.row();
    // Note: RustPython's Stmt doesn't have end_location in the same way
    // We'll estimate based on the start line for now
    // This will be refined when we extract source
    (start_line, start_line)
}

/// Extract source lines from start to end (inclusive, 1-indexed)
fn extract_source_lines(source: &str, start: usize, end: usize) -> Result<String> {
    let lines: Vec<&str> = source.lines().collect();

    if start < 1 || start > lines.len() {
        anyhow::bail!("Start line {} out of range (1-{})", start, lines.len());
    }

    let end = end.min(lines.len());

    Ok(lines[(start - 1)..end].join("\n"))
}

/// Calculate CRC32 checksum for a string
///
/// Returns a signed i32 to match pytest-testmon's format
pub fn calculate_checksum(source: &str) -> i32 {
    let mut hasher = Hasher::new();
    hasher.update(source.as_bytes());
    hasher.finalize() as i32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_function() {
        let source = r#"
def add(a, b):
    return a + b
"#;
        let blocks = parse_module_internal(source).unwrap();

        // Should have module + function
        assert!(blocks.len() >= 2);
        assert_eq!(blocks[0].name, "<module>");

        // Find the function block
        let func_block = blocks.iter().find(|b| b.name == "add").unwrap();
        assert_eq!(func_block.block_type, "function");
    }

    #[test]
    fn test_parse_class_with_methods() {
        let source = r#"
class Calculator:
    def add(self, a, b):
        return a + b

    def subtract(self, a, b):
        return a - b
"#;
        let blocks = parse_module_internal(source).unwrap();

        // Should have: module + class + 2 methods
        assert!(blocks.len() >= 4);
        assert!(blocks.iter().any(|b| b.name == "Calculator" && b.block_type == "class"));
        assert!(blocks.iter().any(|b| b.name == "add" && b.block_type == "function"));
        assert!(blocks.iter().any(|b| b.name == "subtract" && b.block_type == "function"));
    }

    #[test]
    fn test_parse_async_function() {
        let source = r#"
async def fetch_data():
    return await get_data()
"#;
        let blocks = parse_module_internal(source).unwrap();

        assert!(blocks.len() >= 2);
        let async_func = blocks.iter().find(|b| b.name == "fetch_data").unwrap();
        assert_eq!(async_func.block_type, "async_function");
    }

    #[test]
    fn test_checksum_stability() {
        let source = "def foo(): pass";
        let checksum1 = calculate_checksum(source);
        let checksum2 = calculate_checksum(source);

        assert_eq!(checksum1, checksum2);
    }

    #[test]
    fn test_checksum_changes_with_content() {
        let source1 = "def foo(): pass";
        let source2 = "def foo(): return 1";

        let checksum1 = calculate_checksum(source1);
        let checksum2 = calculate_checksum(source2);

        assert_ne!(checksum1, checksum2);
    }

    #[test]
    fn test_parse_nested_functions() {
        let source = r#"
def outer():
    def inner():
        pass
    return inner
"#;
        let blocks = parse_module_internal(source).unwrap();

        // Should have: module + outer + inner
        assert!(blocks.len() >= 3);
        assert!(blocks.iter().any(|b| b.name == "outer"));
        assert!(blocks.iter().any(|b| b.name == "inner"));
    }

    #[test]
    fn test_parse_invalid_syntax() {
        let source = "def foo(";
        let result = parse_module_internal(source);

        assert!(result.is_err());
    }
}
