// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { getIncomers, getOutgoers, isNode } from 'react-flow-renderer';

export const HighlightStatuses = Object.freeze({
    Highlighted: Symbol("Highlighted"),
    AltHighlighted: Symbol("AltHighlighted"),
    NotHighlighted: Symbol("NotHighlighted"),
    Default: Symbol("Default")
});

export function isColumn(node) {
    switch (node.data.pythonType) {
        case "TableColumn":
        case "StarColumn":
        case "JoinInfo":
        case "WhereInfo":
            return true;
        default:
            return false;
    }
}

export function isTable(node) {
    switch (node.data.pythonType) {
        case "SelectTable":
        case "UnnestTable":
        case "ExternalTable":
        case "CycleBreakerTable":
            return true;
        default:
            return false;
    }
}

export function isDomainTextInfo(node) {
    return node.data.pythonType === "TextInfoPanel" && node.data.panelName === "domain_info";
}

function getAllIncomers(node, flow) {
    const nodes = flow.getNodes();
    const edges = flow.getEdges();
    var queue = [node];
    var incomers = new Set();
    while (queue.length > 0) {
        var n = queue.shift();
        if (!incomers.has(n.id)) {
            incomers.add(n.id);
            queue = [...queue, ...getIncomers(n, nodes, edges)];
        }
    }
    return incomers;
}

function getAllOutgoers(node, flow) {
    const nodes = flow.getNodes();
    const edges = flow.getEdges();
    var queue = [node];
    var outgoers = new Set();
    while (queue.length > 0) {
        var n = queue.shift();
        if (!outgoers.has(n.id)) {
            outgoers.add(n.id);
            queue = [...queue, ...getOutgoers(n, nodes, edges)];
        }
    }
    return outgoers;
}

export function getRelatedNodeIDs(node, flow) {
    var relatedNodes = new Set();
    relatedNodes.add(node.id);
    getAllIncomers(node, flow).forEach((n) => { relatedNodes.add(n) });
    getAllOutgoers(node, flow).forEach((n) => { relatedNodes.add(n) });
    return relatedNodes;
}

function getChildren(node, flow) {
    const nodes = flow.getNodes()
    var queue = [node];
    var children = [];
    while (queue.length > 0) {
        var n = queue.shift();
        for (let i = 0; i < nodes.length; i++) {
            if (nodes[i].parentNode === n.id) {
                queue.push(nodes[i]);
                children.push(nodes[i]);
            }
        }
    }
    return children;
}

function getParentTableID(node, flow) {
    while (!isTable(node)) {
        node = flow.getNode(node.parentNode);
    }
    return node.id;
}

function getRelatedTableIDs(tableNode, flow) {
    const children = getChildren(tableNode, flow);
    var relatedTables = new Set();
    relatedTables.add(tableNode.id);
    children.forEach((child) => {
        getRelatedNodeIDs(child, flow).forEach((nodeID) => {
            const n = flow.getNode(nodeID);
            relatedTables.add(getParentTableID(n, flow));
        });
    })
    return relatedTables;
}

export function getNodeIDsRelatedToTable(tableNode, flow) {
    const relatedTables = getRelatedTableIDs(tableNode, flow);
    var nodeIDs = new Set();
    flow.getNodes().forEach((n) => {
        if (relatedTables.has(getParentTableID(n, flow))) {
            nodeIDs.add(n.id);
        }
    })
    return nodeIDs;
}

export function getDomainNodeIDs(node, flow) {
    const domain = node.data.domain;
    var nodeIDs = new Set();
    flow.getNodes().forEach((n) => {
        if (n.data.domain === domain) {
            nodeIDs.add(n.id);
        }
    })
    return nodeIDs;
}

export function checkHighlight(element, nodesToHighlight) {
    if (isNode(element)) {
        return nodesToHighlight.has(element.id);
    } else {
        return nodesToHighlight.has(element.source) && nodesToHighlight.has(element.target);
    }
}

