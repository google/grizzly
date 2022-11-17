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

//Color Palette:
//rgba(218, 232, 252, 1) - blue
//rgba(213, 232, 212, 1) - green
//rgba(255, 230, 204, 1) - orange
//rgba(255, 242, 204, 1) - yellow
//rgba(248, 206, 204, 1) - red
//rgba(225, 213, 231, 1) - violet
//rgba(245, 245, 245, 1) - grey


import {HighlightStatuses} from "./HighlightUtils";

export const config = Object.freeze({
    colors: {
        ColumnColor: "rgba(245, 245, 245, 1)",
        ColumnContainerColor: "rgba(218, 232, 252, 1)",
        PhysicalTableColor: "rgba(248, 206, 204, 1)",
        VirtualTableColor: "rgba(255, 242, 204, 1)",
        ExternalTableColor: "rgba(225, 213, 231, 1)",
        CycleBreakerTableColor: "rgba(255, 230, 204, 1)",
        
        DefaultNodeColor: "rgba(245, 245, 245, 1)",
        HighlightedNodeColor: "rgba(213, 232, 212, 1)",
        AltHighlightedNodeColor: "rgba(248, 50, 50, 1)",
        DefaultEdgeColor: "rgba(69, 69, 69, 0.5)",
        HighlightedEdgeColor: "rgba(255, 0, 0, 1)",
    },
    opacities: {
        FocusOpacity: 1,
        OutOfFocusOpacity: 0.25,
    }
});

function getNodeColor(node) {
    if (node.data.highlightStatus === HighlightStatuses.Highlighted) {
        return config.colors.HighlightedNodeColor;
    }
    else if (node.data.highlightStatus === HighlightStatuses.AltHighlighted) {
        return config.colors.AltHighlightedNodeColor;
    } 
    else {
        switch (node.data.pythonType) {
            case "TableColumn":
            case "StarColumn":
            case "JoinInfo":
            case "WhereInfo":
                return config.colors.ColumnColor;
            case "ColumnContainer":
                return config.colors.ColumnContainerColor;
            case "SelectTable":
            case "UnnestTable":
                if (node.data.tablePhysical) {
                    return config.colors.PhysicalTableColor;
                } else {
                    return config.colors.VirtualTableColor;
                }
            case "ExternalTable":
                return config.colors.ExternalTableColor;
            case "CycleBreakerTable":
                return config.colors.CycleBreakerTableColor;
            default:
                return config.colors.DefaultNodeColor;
        }
    }
}

function getNodeSelectableStatus(node) {
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

function getNodeDraggableStatus(node) {
    switch (node.data.pythonType) {
        case "SelectTable":
        case "ExternalTable":
        case "UnionAllTable":
        case "CycleBreakerTable":
            return undefined;
        default:
            return false;
    }
}

export function applyNodeStyle(node) {
    node.style = {
        ...node.style,
        backgroundColor: getNodeColor(node),
        opacity: (node.data.highlightStatus === HighlightStatuses.NotHighlighted) ? config.opacities.OutOfFocusOpacity : config.opacities.FocusOpacity,
        border: "none",
        borderRadius: "7px 7px 7px 7px",
        boxShadow: "0 2px 4px rgba(0, 0, 0, 0.16), 0 2px 4px rgba(0, 0, 0, 0.23)",
    }
    return node;
}

export function applyEdgeStyle(edge) {
    edge.animated = (edge.data.highlightStatus === HighlightStatuses.Highlighted);
    edge.style = {
        ...edge.style,
        stroke: (edge.data.highlightStatus === HighlightStatuses.Highlighted) ? config.colors.HighlightedEdgeColor : config.colors.DefaultEdgeColor,
        opacity: (edge.data.highlightStatus === HighlightStatuses.NotHighlighted) ? config.opacities.OutOfFocusOpacity : config.opacities.FocusOpacity
    }
    return edge;
}

export function setDefaultNodeProperties(node) {
    node.data.highlightStatus = HighlightStatuses.HighlightNotActive;
    node.connectable = false;
    node.dragHandle = '.custom-drag-handle';
    node.draggable = getNodeDraggableStatus(node);
    node.data.draggable = getNodeDraggableStatus(node) !== false;
    node.selectable = getNodeSelectableStatus(node);
    return node;
}

export function setDefaultEdgeProperties(edge) {
    edge.data.highlightStatus = HighlightStatuses.HighlightNotActive;
    edge.zIndex = 1;
    return edge;
}