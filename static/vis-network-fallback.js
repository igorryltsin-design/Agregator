// Minimal vis-network fallback that exposes window.vis with DataSet and Network
// It doesn't implement full vis functionality but provides enough to avoid runtime errors
(function(window){
  if(window.vis) return; // don't override if real lib present
  function DataSet(items){
    this.items = (items||[]).slice();
  }
  DataSet.prototype.get = function(){ return this.items; };
  DataSet.prototype.add = function(){ /* no-op */ };
  DataSet.prototype.update = function(){ /* no-op */ };

  function Network(container, data, options){
    // Attempt to render using existing SVG fallback if available
    try {
      if(window.renderFallbackGraph) {
        // convert vis-like data to expected shape
        var nodes = (data.nodes && data.nodes.get) ? data.nodes.get() : (data.nodes || []);
        var edges = (data.edges && data.edges.get) ? data.edges.get() : (data.edges || []);
        // normalize to {id,label,type}
        var normNodes = nodes.map(function(n){ return { id: n.id, label: n.label || n.title || String(n.id), type: n.group || n.type || 'work' }; });
        var normEdges = edges.map(function(e){ return { from: e.from, to: e.to, label: e.label || '' }; });
        window.renderFallbackGraph(container, { nodes: normNodes, edges: normEdges });
      } else {
        container.innerText = 'Graph fallback: renderFallbackGraph not found.';
      }
    } catch(e) {
      console.error('vis fallback error', e);
      container.innerText = 'Graph rendering error.';
    }
  }

  window.vis = { DataSet: DataSet, Network: Network };
})(window);
