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

import { useSearchParams } from 'react-router-dom';
import { useRef } from 'react';

import ReactFlow, {
    Controls,
    Background,
    useNodesState,
    useEdgesState,
    useReactFlow,
} from 'react-flow-renderer';

import CustomNode from './CustomComponents/CustomNode';
import {
    HighlightStatuses,
    isColumn,
    isTable,
    isDomainTextInfo,
    getRelatedNodeIDs,
    getNodeIDsRelatedToTable,
    getDomainNodeIDs,
    checkHighlight
} from "./Utils/HighlightUtils";
import {
    applyNodeStyle,
    applyEdgeStyle,
    setDefaultNodeProperties,
    setDefaultEdgeProperties
} from "./Utils/StyleUtils"
import FlowControls from './CustomComponents/FlowControls';


const nodeTypes = { default: CustomNode };

const Flow = ({debug}) => {
    const [searchParams, setSearchParams] = useSearchParams();
    const [nodes, setNodes, onNodesChange] = useNodesState();
    const [edges, setEdges, onEdgesChange] = useEdgesState();
    const reactFlowInstance = useReactFlow();
    const tableSearchForm = useRef();

    const applyFunctionToNodes = (fnc) => {
        setNodes((prevNodes) => {
            return prevNodes?.map((node) => {
                return fnc(node);
            })
        });
    }

    const applyFunctionToEdges = (fnc) => {
        setEdges((prevEdges) => {
            return prevEdges?.map((edge) => {
                return fnc(edge);
            })
        });
    }

    const highlightColumnPath = (columnNode) => {
        const nodesToHighlight = getRelatedNodeIDs(columnNode, reactFlowInstance);

        applyFunctionToNodes((node) => {
            if (node.id === columnNode.id) {
                node.data.highlightStatus = HighlightStatuses.AltHighlighted;
            } else {
                const highlight = checkHighlight(node, nodesToHighlight);
                node.data.highlightStatus = highlight ? HighlightStatuses.Highlighted : HighlightStatuses.NotHighlighted;
            }
            node = applyNodeStyle(node);
            return node;
        });
        applyFunctionToEdges((edge) => {
            const highlight = checkHighlight(edge, nodesToHighlight);
            edge.data.highlightStatus = highlight ? HighlightStatuses.Highlighted : HighlightStatuses.NotHighlighted;
            edge = applyEdgeStyle(edge);
            return edge;
        });
    }

    const highlightRelatedTables = (tableNode) => {
        const nodesToHighlight = getNodeIDsRelatedToTable(tableNode, reactFlowInstance);

        applyFunctionToNodes((node) => {
            if (node.id === tableNode.id) {
                node.data.highlightStatus = HighlightStatuses.Highlighted;
            } else {
                const highlight = checkHighlight(node, nodesToHighlight);
                node.data.highlightStatus = highlight ? HighlightStatuses.Default : HighlightStatuses.NotHighlighted;
            }
            node = applyNodeStyle(node);
            return node;
        });
        applyFunctionToEdges((edge) => {
            const highlight = checkHighlight(edge, nodesToHighlight);
            edge.data.highlightStatus = highlight ? HighlightStatuses.Default : HighlightStatuses.NotHighlighted;
            edge = applyEdgeStyle(edge);
            return edge;
        });
    }

    const highlightDomain = (domainNode) => {
        const nodesToHighlight = getDomainNodeIDs(domainNode, reactFlowInstance);
        const domain = domainNode.data.domain;

        applyFunctionToNodes((node) => {
            if (isDomainTextInfo(node) && node.data.domain === domain) {
                node.data.highlightStatus = HighlightStatuses.Highlighted;
            } else {
                const highlight = checkHighlight(node, nodesToHighlight);
                node.data.highlightStatus = highlight ? HighlightStatuses.Default : HighlightStatuses.NotHighlighted;
            }
            node = applyNodeStyle(node);
            return node;
        });
        applyFunctionToEdges((edge) => {
            const highlight = checkHighlight(edge, nodesToHighlight);
            edge.data.highlightStatus = highlight ? HighlightStatuses.Default : HighlightStatuses.NotHighlighted;
            edge = applyEdgeStyle(edge);
            return edge;
        });
    }

    const resetHighlightStatuses = (resetTableSearchValue=true) => {
        if (resetTableSearchValue){
            tableSearchForm.current.resetTable();
        }
        applyFunctionToNodes((node) => {
            node.data.highlightStatus = HighlightStatuses.Default;
            node = applyNodeStyle(node);
            return node;
        });
        applyFunctionToEdges((edge) => {
            edge.data.highlightStatus = HighlightStatuses.Default;
            edge = applyEdgeStyle(edge);
            return edge;
        });
    }

    const loadFlow = (nodesJson, edgesJson, fp) => {
        setSearchParams(fp);
        setNodes(nodesJson.map((node) => {
            return setDefaultNodeProperties(node);
        }));
        setEdges(edgesJson.map((edge) => {
            return setDefaultEdgeProperties(edge);
        }));
        applyFunctionToNodes(applyNodeStyle);
        applyFunctionToEdges(applyEdgeStyle);

        let tableList = [];
        nodesJson.forEach((node) => {
            if (isTable(node)) {
                tableList.push(node.id);
            }
        })
        tableSearchForm.current.updateTableList(tableList);
    }

    const clearFlow = () => {
        tableSearchForm.current.updateTableList([]);
        setSearchParams([]);
        setNodes([]);
        setEdges([]);
    }

    const focusOnTable = (tableName) => {
        const table = reactFlowInstance.getNode(tableName);
        if (table && isTable(table)) {
            const bounds = {x: table.position.x, y: table.position.y, width: table.style.width, height: table.style.height};
            reactFlowInstance.fitBounds(bounds);
            handleNodeOnClick(table, false);
        }
    }

    const handleNodeOnClick = (node, resetTableSearchValue=true) => {
        resetHighlightStatuses(resetTableSearchValue);
        if (isColumn(node)) {
            highlightColumnPath(node);
        } else if (isTable(node)) {
            highlightRelatedTables(node);
        } else if (isDomainTextInfo(node)) {
            highlightDomain(node);
        }    
    }

    const handleEdgeClick = (edge) => {
        const type = searchParams.get("type");
        if (type === "PROJECT LEVEL" || type === "DOMAIN LEVEL") {
            const targetNode = reactFlowInstance.getNode(edge.target);
            const jobBuildID = targetNode.data.target_table;
            const domain = targetNode.data.domain;
            const project = searchParams.get("project");
            const datetime = searchParams.get("datetime");
            const url = `/?type=QUERY LEVEL&project=${project}&datetime=${datetime}&domain=${domain}&job_build_id=${jobBuildID}`;
            window.open(url, "_blank");
        }
    }

    return (
        <ReactFlow
            nodes={nodes}
            edges={edges}
            nodeTypes={nodeTypes}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            selectNodesOnDrag={false}
            onNodeClick={(_event, node) => { handleNodeOnClick(node) }}
            onEdgeClick={(_event, edge) => { handleEdgeClick(edge) }}
            onPaneClick={() => { resetHighlightStatuses() }}
            minZoom={0.01}
        >
            <FlowControls 
                debug={debug}
                searchParams={searchParams}
                loadFlow={loadFlow}
                clearFlow={clearFlow}
                focusOnTable={focusOnTable}
                TSRef={tableSearchForm}
            />
            <Controls style={{position: "fixed"}}/>
            <Background variant="lines" gap={20} size={1} />
        </ReactFlow>
    );
};

export default Flow;