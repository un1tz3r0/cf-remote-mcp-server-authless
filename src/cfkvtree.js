/**
 * CloudflareKV Tree Storage Implementation
 * Provides hierarchical tree storage on top of Cloudflare KV
 * 
 * Credit is mostly due to Anthropic's Claude 4 Opus model. **Thanks, Claude!**
 * Reviewed by Victor Condino <un1tz3r0@gmail.com>
 */
class KVTreeStorage {
  constructor(kvNamespace, options = {}) {
    this.kv = kvNamespace;
    this.globalPrefix = options.prefix || 'tree';
    this.maxRetries = options.maxRetries || 5;
    this.baseRetryDelay = options.baseRetryDelay || 1000;
    this.encoder = new TextEncoder();
    this.algorithm = 'SHA-256';
  }

  /**
   * Generate consistent hash for a key path
   * Uses SHA-256 for optimal performance on modern JS runtimes
   */
  async hashPath(pathArray) {
    // Ensure consistent ordering and serialization
    const pathString = pathArray.map(key => String(key)).join('::');
    const data = this.encoder.encode(pathString);
    const hashBuffer = await crypto.subtle.digest(this.algorithm, data);
    
    // Convert to hex string
    return Array.from(new Uint8Array(hashBuffer))
      .map(b => b.toString(16).padStart(2, '0'))
      .join('');
  }

  /**
   * Generate storage key with prefix and suffix
   */
  async createStorageKey(pathArray, suffix) {
    const hash = await this.hashPath(pathArray);
    return `${this.globalPrefix}-${hash}-${suffix}`;
  }

  /**
   * Retry wrapper for KV operations with exponential backoff
   * Handles KV's 1-write-per-second-per-key limitation
   */
  async withRetry(operation, context = 'operation') {
    let lastError;
    
    for (let attempt = 0; attempt < this.maxRetries; attempt++) {
      try {
        return await operation();
      } catch (error) {
        lastError = error;
        
        // Check if it's a rate limit error (429) or temporary failure
        if (this.isRetryableError(error) && attempt < this.maxRetries - 1) {
          const delay = this.calculateBackoffDelay(attempt);
          await this.sleep(delay);
          continue;
        }
        
        throw error;
      }
    }
    
    throw new Error(`${context} failed after ${this.maxRetries} attempts: ${lastError.message}`);
  }

  /**
   * Determine if error is retryable
   */
  isRetryableError(error) {
    // KV rate limiting, network errors, or temporary unavailability
    return error.status === 429 || 
           error.status >= 500 || 
           error.name === 'NetworkError' ||
           error.message.includes('timeout');
  }

  /**
   * Calculate exponential backoff delay with jitter
   */
  calculateBackoffDelay(attempt) {
    const baseDelay = this.baseRetryDelay * Math.pow(2, attempt);
    const jitter = Math.random() * 0.1 * baseDelay; // 10% jitter
    return Math.min(baseDelay + jitter, 30000); // Cap at 30 seconds
  }

  /**
   * Sleep utility
   */
  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * Serialize value for storage
   * Uses deterministic JSON for consistent hashing
   */
  serializeValue(value) {
    if (value === null || value === undefined) {
      return null;
    }
    
    // For objects, ensure consistent key ordering
    if (typeof value === 'object' && !Array.isArray(value)) {
      const sortedObj = {};
      Object.keys(value).sort().forEach(key => {
        sortedObj[key] = value[key];
      });
      return JSON.stringify(sortedObj);
    }
    
    return JSON.stringify(value);
  }

  /**
   * Deserialize value from storage
   */
  deserializeValue(serialized) {
    if (serialized === null || serialized === undefined) {
      return null;
    }
    
    try {
      return JSON.parse(serialized);
    } catch (error) {
      throw new Error(`Failed to deserialize value: ${error.message}`);
    }
  }

  /**
   * Initialize root node if it doesn't exist
   */
  async initializeRoot() {
    const rootPath = [];
    const rootExists = await this.nodeExists(rootPath);
    
    if (!rootExists) {
      await this.createNode(rootPath, null);
    }
  }

  /**
   * Check if a node exists
   */
  async nodeExists(pathArray) {
    try {
      const valueKey = await this.createStorageKey(pathArray, 'value');
      const value = await this.kv.get(valueKey);
      return value !== null;
    } catch (error) {
      return false;
    }
  }

  /**
   * Create a new node
   */
  async createNode(pathArray, value = null) {
    // Prevent creating root twice
    if (pathArray.length === 0) {
      const exists = await this.nodeExists([]);
      if (exists) {
        throw new Error('Root node already exists');
      }
    }

    // Validate parent exists (except for root)
    if (pathArray.length > 0) {
      const parentPath = pathArray.slice(0, -1);
      const parentExists = await this.nodeExists(parentPath);
      if (!parentExists) {
        throw new Error(`Parent node does not exist: ${parentPath.join('/')}`);
      }
    }

    // Create node with atomic-like operations
    await this.withRetry(async () => {
      // Store value
      const valueKey = await this.createStorageKey(pathArray, 'value');
      await this.kv.put(valueKey, this.serializeValue(value));

      // Store parent reference (except for root)
      if (pathArray.length > 0) {
        const parentKey = await this.createStorageKey(pathArray, 'parent');
        const parentPath = pathArray.slice(0, -1);
        await this.kv.put(parentKey, this.serializeValue(parentPath));
      }

      // Initialize empty children list
      const childrenKey = await this.createStorageKey(pathArray, 'children');
      await this.kv.put(childrenKey, this.serializeValue([]));

      // Update parent's children list (except for root)
      if (pathArray.length > 0) {
        await this.addChildToParent(pathArray.slice(0, -1), pathArray[pathArray.length - 1]);
      }
    }, `create node ${pathArray.join('/')}`);
  }

  /**
   * Add child to parent's children list
   */
  async addChildToParent(parentPath, childKey) {
    const childrenKey = await this.createStorageKey(parentPath, 'children');
    
    // Get current children with retry
    const currentChildrenSerialized = await this.withRetry(async () => {
      return await this.kv.get(childrenKey);
    }, 'get parent children');

    const currentChildren = this.deserializeValue(currentChildrenSerialized) || [];
    
    // Add if not already present
    if (!currentChildren.includes(childKey)) {
      currentChildren.push(childKey);
      
      // Update with retry
      await this.withRetry(async () => {
        await this.kv.put(childrenKey, this.serializeValue(currentChildren));
      }, 'update parent children');
    }
  }

  /**
   * Remove child from parent's children list
   */
  async removeChildFromParent(parentPath, childKey) {
    const childrenKey = await this.createStorageKey(parentPath, 'children');
    
    const currentChildrenSerialized = await this.withRetry(async () => {
      return await this.kv.get(childrenKey);
    }, 'get parent children for removal');

    const currentChildren = this.deserializeValue(currentChildrenSerialized) || [];
    const updatedChildren = currentChildren.filter(key => key !== childKey);
    
    await this.withRetry(async () => {
      await this.kv.put(childrenKey, this.serializeValue(updatedChildren));
    }, 'remove child from parent');
  }

  /**
   * Get node value
   */
  async getValue(pathArray) {
    const valueKey = await this.createStorageKey(pathArray, 'value');
    const serializedValue = await this.withRetry(async () => {
      return await this.kv.get(valueKey);
    }, `get value for ${pathArray.join('/')}`);

    if (serializedValue === null) {
      throw new Error(`Node does not exist: ${pathArray.join('/')}`);
    }

    return this.deserializeValue(serializedValue);
  }

  /**
   * Set node value
   */
  async setValue(pathArray, value) {
    // Verify node exists
    const exists = await this.nodeExists(pathArray);
    if (!exists) {
      throw new Error(`Node does not exist: ${pathArray.join('/')}`);
    }

    const valueKey = await this.createStorageKey(pathArray, 'value');
    await this.withRetry(async () => {
      await this.kv.put(valueKey, this.serializeValue(value));
    }, `set value for ${pathArray.join('/')}`);
  }

  /**
   * Get node children
   */
  async getChildren(pathArray) {
    const childrenKey = await this.createStorageKey(pathArray, 'children');
    const serializedChildren = await this.withRetry(async () => {
      return await this.kv.get(childrenKey);
    }, `get children for ${pathArray.join('/')}`);

    if (serializedChildren === null) {
      throw new Error(`Node does not exist: ${pathArray.join('/')}`);
    }

    return this.deserializeValue(serializedChildren) || [];
  }

  /**
   * Get complete node information
   */
  async getNode(pathArray) {
    const [value, children] = await Promise.all([
      this.getValue(pathArray),
      this.getChildren(pathArray)
    ]);

    return {
      path: pathArray,
      value,
      children,
      hasChildren: children.length > 0
    };
  }

  /**
   * Delete a node and all its descendants
   * Implements two-phase deletion for consistency
   */
  async deleteNode(pathArray) {
    // Prevent deleting root
    if (pathArray.length === 0) {
      throw new Error('Cannot delete root node');
    }

    // Verify node exists
    const exists = await this.nodeExists(pathArray);
    if (!exists) {
      throw new Error(`Node does not exist: ${pathArray.join('/')}`);
    }

    // Phase 1: Collect all descendants
    const descendantPaths = await this.collectDescendants(pathArray);
    const allPaths = [pathArray, ...descendantPaths];

    try {
      // Phase 2: Delete all nodes (leaf to root order for consistency)
      allPaths.reverse(); // Delete leaves first
      
      for (const path of allPaths) {
        await this.deleteSingleNode(path);
      }

      // Update parent's children list
      const parentPath = pathArray.slice(0, -1);
      const childKey = pathArray[pathArray.length - 1];
      await this.removeChildFromParent(parentPath, childKey);
      
    } catch (error) {
      throw new Error(`Failed to delete node ${pathArray.join('/')}: ${error.message}`);
    }
  }

  /**
   * Collect all descendant paths using breadth-first traversal
   */
  async collectDescendants(pathArray) {
    const descendants = [];
    const queue = [pathArray];
    
    while (queue.length > 0) {
      const currentPath = queue.shift();
      
      try {
        const children = await this.getChildren(currentPath);
        
        for (const childKey of children) {
          const childPath = [...currentPath, childKey];
          descendants.push(childPath);
          queue.push(childPath);
        }
      } catch (error) {
        // If we can't get children, node might already be deleted
        continue;
      }
    }
    
    return descendants;
  }

  /**
   * Delete a single node's storage keys
   */
  async deleteSingleNode(pathArray) {
    const keys = [
      await this.createStorageKey(pathArray, 'value'),
      await this.createStorageKey(pathArray, 'children')
    ];

    // Add parent key for non-root nodes
    if (pathArray.length > 0) {
      keys.push(await this.createStorageKey(pathArray, 'parent'));
    }

    // Delete all keys with retry
    for (const key of keys) {
      await this.withRetry(async () => {
        await this.kv.delete(key);
      }, `delete key ${key}`);
    }
  }

  /**
   * Move a subtree to a new parent
   * Implements copy-then-delete pattern for atomicity
   */
  async moveNode(sourcePath, targetParentPath, newKey) {
    // Validation
    if (sourcePath.length === 0) {
      throw new Error('Cannot move root node');
    }

    const sourceExists = await this.nodeExists(sourcePath);
    if (!sourceExists) {
      throw new Error(`Source node does not exist: ${sourcePath.join('/')}`);
    }

    const targetParentExists = await this.nodeExists(targetParentPath);
    if (!targetParentExists) {
      throw new Error(`Target parent does not exist: ${targetParentPath.join('/')}`);
    }

    const newPath = [...targetParentPath, newKey];
    const newExists = await this.nodeExists(newPath);
    if (newExists) {
      throw new Error(`Target path already exists: ${newPath.join('/')}`);
    }

    try {
      // Phase 1: Copy subtree to new location
      await this.copySubtree(sourcePath, targetParentPath, newKey);
      
      // Phase 2: Delete original subtree
      await this.deleteNode(sourcePath);
      
    } catch (error) {
      // Attempt cleanup of partial copy
      try {
        await this.deleteNode(newPath);
      } catch (cleanupError) {
        // Log cleanup failure but don't mask original error
      }
      
      throw new Error(`Failed to move node: ${error.message}`);
    }
  }

  /**
   * Copy a subtree to a new location
   */
  async copySubtree(sourcePath, targetParentPath, newKey) {
    const sourceNode = await this.getNode(sourcePath);
    const newPath = [...targetParentPath, newKey];
    
    // Create new node
    await this.createNode(newPath, sourceNode.value);
    
    // Recursively copy children
    for (const childKey of sourceNode.children) {
      const childSourcePath = [...sourcePath, childKey];
      await this.copySubtree(childSourcePath, newPath, childKey);
    }
  }

  /**
   * Traverse tree with callback function
   * Supports both depth-first and breadth-first traversal
   */
  async traverse(pathArray = [], callback, options = {}) {
    const { 
      strategy = 'depth-first', // 'depth-first' or 'breadth-first'
      maxDepth = Infinity,
      includeInternal = true 
    } = options;
    
    if (!await this.nodeExists(pathArray)) {
      throw new Error(`Starting node does not exist: ${pathArray.join('/')}`);
    }

    if (strategy === 'breadth-first') {
      await this.breadthFirstTraverse(pathArray, callback, maxDepth, includeInternal);
    } else {
      await this.depthFirstTraverse(pathArray, callback, maxDepth, includeInternal, 0);
    }
  }

  /**
   * Depth-first traversal implementation
   */
  async depthFirstTraverse(pathArray, callback, maxDepth, includeInternal, currentDepth) {
    if (currentDepth > maxDepth) return;
    
    const node = await this.getNode(pathArray);
    
    // Call callback for this node
    const shouldContinue = await callback(node, currentDepth);
    if (shouldContinue === false) return;
    
    // Traverse children
    for (const childKey of node.children) {
      const childPath = [...pathArray, childKey];
      await this.depthFirstTraverse(childPath, callback, maxDepth, includeInternal, currentDepth + 1);
    }
  }

  /**
   * Breadth-first traversal implementation
   */
  async breadthFirstTraverse(pathArray, callback, maxDepth, includeInternal) {
    const queue = [{ path: pathArray, depth: 0 }];
    
    while (queue.length > 0) {
      const { path, depth } = queue.shift();
      
      if (depth > maxDepth) continue;
      
      const node = await this.getNode(path);
      
      // Call callback for this node
      const shouldContinue = await callback(node, depth);
      if (shouldContinue === false) continue;
      
      // Add children to queue
      for (const childKey of node.children) {
        const childPath = [...path, childKey];
        queue.push({ path: childPath, depth: depth + 1 });
      }
    }
  }

  /**
   * Find nodes matching a predicate
   */
  async findNodes(predicate, startPath = []) {
    const results = [];
    
    await this.traverse(startPath, async (node, depth) => {
      if (await predicate(node, depth)) {
        results.push(node);
      }
      return true; // Continue traversal
    });
    
    return results;
  }

  /**
   * Get tree statistics
   */
  async getStats(pathArray = []) {
    let nodeCount = 0;
    let leafCount = 0;
    let maxDepth = 0;
    let totalSize = 0;
    
    await this.traverse(pathArray, async (node, depth) => {
      nodeCount++;
      if (node.children.length === 0) leafCount++;
      maxDepth = Math.max(maxDepth, depth);
      
      // Estimate size (rough approximation)
      const valueSize = node.value ? JSON.stringify(node.value).length : 0;
      totalSize += valueSize;
      
      return true;
    });
    
    return {
      nodeCount,
      leafCount,
      internalNodeCount: nodeCount - leafCount,
      maxDepth,
      estimatedSize: totalSize
    };
  }

  /**
   * Batch operations for efficiency
   */
  async batchCreateNodes(nodeSpecs) {
    // Sort by path depth to ensure parents exist before children
    const sortedSpecs = nodeSpecs.sort((a, b) => a.path.length - b.path.length);
    
    for (const spec of sortedSpecs) {
      await this.createNode(spec.path, spec.value);
    }
  }

  /**
   * Export tree to JSON structure
   */
  async exportTree(pathArray = []) {
    const node = await this.getNode(pathArray);
    
    const result = {
      value: node.value,
      children: {}
    };
    
    for (const childKey of node.children) {
      const childPath = [...pathArray, childKey];
      result.children[childKey] = await this.exportTree(childPath);
    }
    
    return result;
  }

  /**
   * Import tree from JSON structure
   */
  async importTree(treeData, pathArray = []) {
    // Create/update current node
    if (pathArray.length === 0) {
      await this.initializeRoot();
      if (treeData.value !== undefined) {
        await this.setValue([], treeData.value);
      }
    } else {
      await this.createNode(pathArray, treeData.value);
    }
    
    // Import children
    if (treeData.children) {
      for (const [childKey, childData] of Object.entries(treeData.children)) {
        const childPath = [...pathArray, childKey];
        await this.importTree(childData, childPath);
      }
    }
  }
}

// Export for use in Cloudflare Workers
export default KVTreeStorage;

// Example usage:
/*
// Initialize the tree storage
const tree = new KVTreeStorage(KV_NAMESPACE, { 
  prefix: 'mytree',
  maxRetries: 3 
});

// Initialize root
await tree.initializeRoot();

// Create some nodes
await tree.createNode(['documents'], 'Documents folder');
await tree.createNode(['documents', 'reports'], 'Reports folder');
await tree.createNode(['documents', 'reports', '2025'], 'Annual report data');

// Read values
const value = await tree.getValue(['documents', 'reports']);
console.log(value); // 'Reports folder'

// Traverse the tree
await tree.traverse([], async (node, depth) => {
  console.log('  '.repeat(depth) + node.path.join('/') + ': ' + node.value);
  return true; // Continue traversal
});

// Get tree statistics
const stats = await tree.getStats();
console.log('Tree has', stats.nodeCount, 'nodes');

// Move a subtree
await tree.moveNode(['documents', 'reports'], ['archive'], 'old-reports');

// Export tree to JSON
const exported = await tree.exportTree();
console.log(JSON.stringify(exported, null, 2));
*/
