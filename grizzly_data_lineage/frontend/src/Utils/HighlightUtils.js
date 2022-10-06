import { getIncomers, getOutgoers } from 'react-flow-renderer';

export const HighlightStatuses = Object.freeze({
    Highlighted: Symbol("Highlighted"),
    NotHighlighted: Symbol("NotHighlighted"),
    HighlightNotActive: Symbol("HighlightNotActive")
});

export function getAllIncomers(referenceNode, nodes, edges) {
    var queue = [referenceNode];
    var incomers = [referenceNode];
    while (queue.length > 0) {
        var element = queue.shift();
        incomers = [...incomers, element.id]
        getIncomers(element, nodes, edges).forEach(e => { queue.push(e) });
    }
    return incomers;
}

export function getAllOutgoers(referenceNode, nodes, edges) {
    var queue = [referenceNode];
    var outgoers = [referenceNode];
    while (queue.length > 0) {
        var element = queue.shift();
        outgoers = [...outgoers, element.id]
        getOutgoers(element, nodes, edges).forEach(e => { queue.push(e) });
    }
    return outgoers;
}

export const getNodePathHighlightStatus = (node, referenceNode, incomerIds, outgoerIds) => {
    const highlight = (
        node.id === referenceNode.id ||
        incomerIds.includes(node.id) ||
        outgoerIds.includes(node.id)
    );
    return highlight ? HighlightStatuses.Highlighted : HighlightStatuses.NotHighlighted;
}

export const getEdgePathHighlightStatus = (edge, referenceNode, incomerIds, outgoerIds) => {
    const highlight = ((
        incomerIds.includes(edge.source) &&
        (incomerIds.includes(edge.target) || referenceNode.id === edge.target)
    ) ||
    (
        outgoerIds.includes(edge.target) &&
        (outgoerIds.includes(edge.source) || referenceNode.id === edge.source)
    ));
    return highlight ? HighlightStatuses.Highlighted : HighlightStatuses.NotHighlighted;
}

export const getAllDomainElements = (domain, nodes, edges) => {
    const domainElements = [
        ...nodes.filter((n) => {return n.data.domain === domain}),
        ...edges.filter((e) => {return e.target.domain === domain && e.source.domain === domain})
    ]
    return domainElements.map((e) => {return e.id});
}

export const getNodeDomainHighlightStatus = (node, domainElements) => {
    const highlight = domainElements.includes(node.id);
    return highlight ? HighlightStatuses.HighlightNotActive : HighlightStatuses.NotHighlighted;
}

export const getEdgeDomainHighlightStatus = (edge, domainElements) => {
    const highlight = domainElements.includes(edge.source) && domainElements.includes(edge.target);
    return highlight ? HighlightStatuses.HighlightNotActive : HighlightStatuses.NotHighlighted;
}