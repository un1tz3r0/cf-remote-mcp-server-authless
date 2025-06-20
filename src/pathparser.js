// Escape method constants
const EscapeMethod = {
    BACKSLASH_ESCAPES: 'BACKSLASH_ESCAPES',           // \n, \t, \r, \\, \/, \xHH
    HTML_NAMED_ENTITIES: 'HTML_NAMED_ENTITIES',       // &amp;, &lt;, &gt;, etc.
    HTML_NUMERIC_ENTITIES: 'HTML_NUMERIC_ENTITIES',   // &#123;, &#x7B;
    CUSTOM_UNICODE_ENTITIES: 'CUSTOM_UNICODE_ENTITIES', // &uHHHH;
    URL_ENCODING: 'URL_ENCODING'                      // %HH
};

class PathParser {
    constructor(enabledMethods = Object.values(EscapeMethod)) {
        // Convert array to Set for O(1) lookup
        this.enabledMethods = new Set(enabledMethods);
        
        // HTML entity mapping for common named entities
        this.htmlEntities = {
            'amp': '&',
            'lt': '<',
            'gt': '>',
            'quot': '"',
            'apos': "'",
            'nbsp': '\u00A0',
            'copy': '©',
            'reg': '®',
            'trade': '™',
            'hellip': '…',
            'mdash': '—',
            'ndash': '–',
            'lsquo': "'",
            'rsquo': "'",
            'ldquo': '"',
            'rdquo': '"',
            'bull': '•',
            'deg': '°',
            'plusmn': '±',
            'times': '×',
            'divide': '÷'
        };
    }
    
    // Parse a path string into components
    parsePath(pathString) {
        const components = [];
        let currentComponent = '';
        let i = 0;
        
        while (i < pathString.length) {
            const char = pathString[i];
            
            if (char === '\\' && this.enabledMethods.has(EscapeMethod.BACKSLASH_ESCAPES)) {
                // Handle backslash escapes
                if (i + 1 >= pathString.length) {
                    // Trailing backslash, treat as literal
                    currentComponent += char;
                    i++;
                    continue;
                }
                
                const nextChar = pathString[i + 1];
                
                if (nextChar === 'n') {
                    currentComponent += '\n';
                    i += 2;
                } else if (nextChar === 't') {
                    currentComponent += '\t';
                    i += 2;
                } else if (nextChar === 'r') {
                    currentComponent += '\r';
                    i += 2;
                } else if (nextChar === '\\') {
                    currentComponent += '\\';
                    i += 2;
                } else if (nextChar === '/') {
                    currentComponent += '/';
                    i += 2;
                } else if (nextChar === 'x') {
                    // \xHH hex escape sequence
                    if (i + 3 < pathString.length) {
                        const hexStr = pathString.slice(i + 2, i + 4);
                        if (/^[0-9a-fA-F]{2}$/.test(hexStr)) {
                            const charCode = parseInt(hexStr, 16);
                            currentComponent += String.fromCharCode(charCode);
                            i += 4;
                        } else {
                            // Invalid hex digits, treat as escaped 'x'
                            currentComponent += nextChar;
                            i += 2;
                        }
                    } else {
                        // Not enough characters for \xHH, treat as escaped 'x'
                        currentComponent += nextChar;
                        i += 2;
                    }
                } else {
                    // Any other character after backslash is escaped (literal)
                    currentComponent += nextChar;
                    i += 2;
                }
            } else if (char === '&' && (
                this.enabledMethods.has(EscapeMethod.HTML_NAMED_ENTITIES) ||
                this.enabledMethods.has(EscapeMethod.HTML_NUMERIC_ENTITIES) ||
                this.enabledMethods.has(EscapeMethod.CUSTOM_UNICODE_ENTITIES)
            )) {
                // Handle HTML entities
                const remaining = pathString.slice(i);
                let matched = false;
                
                // Try standard named entities first (&name;)
                if (this.enabledMethods.has(EscapeMethod.HTML_NAMED_ENTITIES)) {
                    const entityMatch = remaining.match(/^&([a-zA-Z][a-zA-Z0-9]*);/);
                    if (entityMatch && this.htmlEntities[entityMatch[1]]) {
                        currentComponent += this.htmlEntities[entityMatch[1]];
                        i += entityMatch[0].length;
                        matched = true;
                    }
                }
                
                // Try numeric entities (&#123; or &#x7B;)
                if (!matched && this.enabledMethods.has(EscapeMethod.HTML_NUMERIC_ENTITIES)) {
                    let entityMatch = remaining.match(/^&#x([0-9a-fA-F]+);/);
                    if (entityMatch) {
                        const codePoint = parseInt(entityMatch[1], 16);
                        try {
                            currentComponent += String.fromCodePoint(codePoint);
                            i += entityMatch[0].length;
                            matched = true;
                        } catch (e) {
                            // Invalid code point, fall through to literal
                        }
                    }
                    
                    if (!matched) {
                        entityMatch = remaining.match(/^&#([0-9]+);/);
                        if (entityMatch) {
                            const codePoint = parseInt(entityMatch[1], 10);
                            try {
                                currentComponent += String.fromCodePoint(codePoint);
                                i += entityMatch[0].length;
                                matched = true;
                            } catch (e) {
                                // Invalid code point, fall through to literal
                            }
                        }
                    }
                }
                
                // Try custom unicode format (&uHHHH...;)
                if (!matched && this.enabledMethods.has(EscapeMethod.CUSTOM_UNICODE_ENTITIES)) {
                    const entityMatch = remaining.match(/^&u([0-9a-fA-F]{2,8});/);
                    if (entityMatch) {
                        const hexStr = entityMatch[1];
                        const codePoint = parseInt(hexStr, 16);
                        try {
                            currentComponent += String.fromCodePoint(codePoint);
                            i += entityMatch[0].length;
                            matched = true;
                        } catch (e) {
                            // Invalid code point, fall through to literal
                        }
                    }
                }
                
                if (!matched) {
                    // Not a recognized entity or method disabled, treat as literal
                    currentComponent += char;
                    i++;
                }
            } else if (char === '%' && this.enabledMethods.has(EscapeMethod.URL_ENCODING)) {
                // Handle URL percent encoding (%HH)
                if (i + 2 < pathString.length) {
                    const hexStr = pathString.slice(i + 1, i + 3);
                    if (/^[0-9a-fA-F]{2}$/.test(hexStr)) {
                        const charCode = parseInt(hexStr, 16);
                        currentComponent += String.fromCharCode(charCode);
                        i += 3;
                    } else {
                        // Invalid hex digits, treat as literal %
                        currentComponent += char;
                        i++;
                    }
                } else {
                    // Not enough characters for %HH, treat as literal %
                    currentComponent += char;
                    i++;
                }
            } else if (char === '/') {
                // Unescaped path separator - split here
                components.push(currentComponent);
                currentComponent = '';
                i++;
            } else {
                // Regular character
                currentComponent += char;
                i++;
            }
        }
        
        // Add the final component
        components.push(currentComponent);
        
        // Remove empty elements, including leading empty element from absolute paths
        const filteredComponents = components.filter((component, index) => {
            // Remove empty strings, but keep them if they're the only element
            if (component === '') {
                // Keep empty string only if it's the sole element (empty path)
                return components.length === 1;
            }
            return true;
        });
        
        return filteredComponents;
    }
    
    // Create a path string from an array of components, using optimal escaping
    createPath(components) {
        // Remove empty components
        const filteredComponents = components.filter(component => component !== '');
        
        // If no components left, return empty string
        if (filteredComponents.length === 0) {
            return '';
        }
        
        // Encode each component
        const encodedComponents = filteredComponents.map(component => this.encodePathComponent(component));
        
        return encodedComponents.join('/');
    }
    
    // Encode a single path component using the most compact enabled escape mechanism
    encodePathComponent(component) {
        let result = '';
        
        for (let i = 0; i < component.length; i++) {
            const char = component[i];
            const charCode = char.charCodeAt(0);
            
            // Characters that must be escaped for path parsing
            if (char === '/') {
                if (this.enabledMethods.has(EscapeMethod.BACKSLASH_ESCAPES)) {
                    result += '\\/';
                } else if (this.enabledMethods.has(EscapeMethod.URL_ENCODING)) {
                    result += '%2F';
                } else if (this.enabledMethods.has(EscapeMethod.HTML_NUMERIC_ENTITIES)) {
                    result += '&#47;';
                } else {
                    // No escape method available, this will cause parsing issues
                    result += char;
                }
            } else if (char === '\\') {
                if (this.enabledMethods.has(EscapeMethod.BACKSLASH_ESCAPES)) {
                    result += '\\\\';
                } else if (this.enabledMethods.has(EscapeMethod.URL_ENCODING)) {
                    result += '%5C';
                } else if (this.enabledMethods.has(EscapeMethod.HTML_NUMERIC_ENTITIES)) {
                    result += '&#92;';
                } else {
                    result += char;
                }
            } else if (char === '\n') {
                if (this.enabledMethods.has(EscapeMethod.BACKSLASH_ESCAPES)) {
                    result += '\\n';
                } else if (this.enabledMethods.has(EscapeMethod.URL_ENCODING)) {
                    result += '%0A';
                } else if (this.enabledMethods.has(EscapeMethod.HTML_NUMERIC_ENTITIES)) {
                    result += '&#10;';
                } else {
                    result += char;
                }
            } else if (char === '\t') {
                if (this.enabledMethods.has(EscapeMethod.BACKSLASH_ESCAPES)) {
                    result += '\\t';
                } else if (this.enabledMethods.has(EscapeMethod.URL_ENCODING)) {
                    result += '%09';
                } else if (this.enabledMethods.has(EscapeMethod.HTML_NUMERIC_ENTITIES)) {
                    result += '&#9;';
                } else {
                    result += char;
                }
            } else if (char === '\r') {
                if (this.enabledMethods.has(EscapeMethod.BACKSLASH_ESCAPES)) {
                    result += '\\r';
                } else if (this.enabledMethods.has(EscapeMethod.URL_ENCODING)) {
                    result += '%0D';
                } else if (this.enabledMethods.has(EscapeMethod.HTML_NUMERIC_ENTITIES)) {
                    result += '&#13;';
                } else {
                    result += char;
                }
            } else if (char === '%') {
                // Escape % to avoid URL decoding confusion when URL_ENCODING is enabled
                if (this.enabledMethods.has(EscapeMethod.URL_ENCODING)) {
                    result += '%25';
                } else if (this.enabledMethods.has(EscapeMethod.HTML_NUMERIC_ENTITIES)) {
                    result += '&#37;';
                } else if (this.enabledMethods.has(EscapeMethod.BACKSLASH_ESCAPES)) {
                    result += '\\%';
                } else {
                    result += char;
                }
            } else if (char === '&') {
                // Escape & to avoid HTML entity confusion when HTML entities are enabled
                if (this.enabledMethods.has(EscapeMethod.HTML_NAMED_ENTITIES)) {
                    result += '&amp;';
                } else if (this.enabledMethods.has(EscapeMethod.URL_ENCODING)) {
                    result += '%26';
                } else if (this.enabledMethods.has(EscapeMethod.HTML_NUMERIC_ENTITIES)) {
                    result += '&#38;';
                } else if (this.enabledMethods.has(EscapeMethod.BACKSLASH_ESCAPES)) {
                    result += '\\&';
                } else {
                    result += char;
                }
            } else if (charCode < 32 || charCode > 126) {
                // Non-printable or non-ASCII characters
                // Choose most compact available encoding
                if (charCode <= 255) {
                    // Single byte character
                    if (this.enabledMethods.has(EscapeMethod.URL_ENCODING)) {
                        result += '%' + charCode.toString(16).toUpperCase().padStart(2, '0');
                    } else if (this.enabledMethods.has(EscapeMethod.BACKSLASH_ESCAPES)) {
                        result += '\\x' + charCode.toString(16).toUpperCase().padStart(2, '0');
                    } else if (this.enabledMethods.has(EscapeMethod.HTML_NUMERIC_ENTITIES)) {
                        result += '&#' + charCode + ';';
                    } else if (this.enabledMethods.has(EscapeMethod.CUSTOM_UNICODE_ENTITIES)) {
                        result += '&u' + charCode.toString(16).toUpperCase().padStart(2, '0') + ';';
                    } else {
                        result += char; // No escape method available
                    }
                } else {
                    // Multi-byte Unicode character
                    if (this.enabledMethods.has(EscapeMethod.HTML_NUMERIC_ENTITIES)) {
                        result += '&#x' + charCode.toString(16).toUpperCase() + ';';
                    } else if (this.enabledMethods.has(EscapeMethod.CUSTOM_UNICODE_ENTITIES)) {
                        result += '&u' + charCode.toString(16).toUpperCase().padStart(4, '0') + ';';
                    } else if (this.enabledMethods.has(EscapeMethod.URL_ENCODING)) {
                        // URL encode as UTF-8 bytes
                        const utf8Bytes = this.stringToUtf8Bytes(char);
                        result += utf8Bytes.map(byte => '%' + byte.toString(16).toUpperCase().padStart(2, '0')).join('');
                    } else {
                        result += char; // No escape method available
                    }
                }
            } else {
                // Regular printable ASCII character
                result += char;
            }
        }
        
        return result;
    }
    
    // Helper function to convert string to UTF-8 bytes
    stringToUtf8Bytes(str) {
        const encoder = new TextEncoder();
        return Array.from(encoder.encode(str));
    }
    
    // Utility function for backward compatibility
    joinPath(components) {
        return this.createPath(components);
    }
}

// Export for use
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { PathParser, EscapeMethod };
}
