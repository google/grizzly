import React, { useState, useRef } from 'react';
import { useSearchParams } from 'react-router-dom';

import ReactFlow, {
    MiniMap,
    Controls,
    Background,
    useNodesState,
    useEdgesState,
    useReactFlow,
} from 'react-flow-renderer';

import DomainLevelForm from './CustomComponents/ControlForms/DomainLevelForm';
import QueryLevelForm from './CustomComponents/ControlForms/QueryLevelForm';
// import OnDemandForm from './CustomComponents/ControlForms/OnDemandForm';
// import TestButtonForm from './CustomComponents/ControlForms/TestButtonForm';
import CustomNode from './CustomComponents/CustomNode';
import {
    HighlightStatuses,
    getAllIncomers,
    getAllOutgoers,
    getNodePathHighlightStatus,
    getEdgePathHighlightStatus,
    getAllDomainElements,
    getNodeDomainHighlightStatus,
    getEdgeDomainHighlightStatus
} from "./Utils/HighlightUtils";
import {
    applyNodeStyle,
    applyEdgeStyle,
    setDefaultNodeProperties,
    setDefaultEdgeProperties
} from "./Utils/StyleUtils"
import {getColorHelpText} from "./Utils/HelpMenu"

const nodeTypes = { default: CustomNode };

const Flow = () => {
    const [nodes, setNodes, onNodesChange] = useNodesState();
    const [edges, setEdges, onEdgesChange] = useEdgesState();
    const reactFlowInstance = useReactFlow();

    const [controlFormLock, setControlFormLock] = useState();
    const updateControlFormLock = (lock) => { setControlFormLock(lock); }

    const [controlsHidden, setControlsHidden] = useState(false);

    const [errorMessage, setErrorMessage] = useState();
    const [currentlyLoadedMessage, setCurrentlyLoadedMessage] = useState();

    const [searchParams, setSearchParams] = useSearchParams();
    const updateSearchParams = (newParams) => { setSearchParams(newParams); }

    const [formParams, setFormParams] = useState(null);

    const setMessage = (name, msg) => {
        switch (name) {
            case "errorMessage":
                setErrorMessage(msg);
                break;
            case "currentlyLoadedMessage":
                setCurrentlyLoadedMessage(msg);
                break;
            default:
                return;
        }
    }
    const getMessage = (name) => {
        switch (name) {
            case "errorMessage":
                return errorMessage ? `Error: ${errorMessage}` : ""
            case "currentlyLoadedMessage":
                return currentlyLoadedMessage ? currentlyLoadedMessage : "No Graph is currently loaded";
            default:
                return;
        }

    }

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

    const highlightNodePath = (referenceNode) => {
        const incomerIds = getAllIncomers(referenceNode, nodes, edges);
        const outgoerIds = getAllOutgoers(referenceNode, nodes, edges);

        applyFunctionToNodes((node) => {
            node.data.highlightStatus = getNodePathHighlightStatus(node, referenceNode, incomerIds, outgoerIds);
            node = applyNodeStyle(node);
            return node;
        });
        applyFunctionToEdges((edge) => {
            edge.data.highlightStatus = getEdgePathHighlightStatus(edge, referenceNode, incomerIds, outgoerIds);
            edge = applyEdgeStyle(edge);
            return edge;
        });
    }

    const highlightDomain = (referenceNode) => {
        const domainElements = getAllDomainElements(referenceNode.data.domain, nodes, edges);
        applyFunctionToNodes((node) => {
            node.data.highlightStatus = getNodeDomainHighlightStatus(node, domainElements);
            node = applyNodeStyle(node);
            return node;
        });
        applyFunctionToEdges((edge) => {
            edge.data.highlightStatus = getEdgeDomainHighlightStatus(edge, domainElements);
            edge = applyEdgeStyle(edge);
            return edge;
        });
    }

    const resetHighlightStatuses = () => {
        applyFunctionToNodes((node) => {
            node.data.highlightStatus = HighlightStatuses.HighlightNotActive;
            node = applyNodeStyle(node);
            return node;
        });
        applyFunctionToEdges((edge) => {
            edge.data.highlightStatus = HighlightStatuses.HighlightNotActive;
            edge = applyEdgeStyle(edge);
            return edge;
        });
    }

    const loadFlow = (nodesJson, edgesJson, fp) => {
        setNodes(nodesJson.map((node) => {
            return setDefaultNodeProperties(node);
        }));
        setEdges(edgesJson.map((edge) => {
            return setDefaultEdgeProperties(edge);
        }));
        applyFunctionToNodes(applyNodeStyle);
        applyFunctionToEdges(applyEdgeStyle);
        setFormParams(fp);
    }

    const clearFlow = (formType) => {
        setMessage("currentlyLoadedMessage", "");
        setMessage("errorMessage", "");
        setSearchParams([]);
        setFormParams(null);
        setNodes([]);
        setEdges([]);
        forms.forEach((form) => {if (form.current.state.formLabel !== formType) {
            form.current.clearInputs();
        }})
    }

    const handleNodeOnClick = (node) => {
        resetHighlightStatuses();
        switch (node.data.pythonType) {
            case "TableColumn":
            case "StarColumn":
            case "JoinInfo":
            case "WhereInfo":
                highlightNodePath(node);
                break;
            case "TextInfoPanel":
                switch (node.data.panelName) {
                    case "domain_info":
                        highlightDomain(node);
                        break;
                    default:
                        return;
                }
                break;
            default:
                return;
        }
    }

    const handleEdgeClick = (edge) => {
        if (formParams?.type === "DOMAIN LEVEL") {
            const targetNode = reactFlowInstance.getNode(edge.target);
            const jobBuildID = targetNode.data.target_table;
            const domain = targetNode.data.domain;
            const project = formParams.project;
            const datetime = formParams.datetime;
            const url = `/?type=QUERY LEVEL&project=${project}&datetime=${datetime}&domain=${domain}&job_build_id=${jobBuildID}`;
            window.open(url, "_blank");
        }


    }

    const domainFormRef = useRef();
    const queryFormRef = useRef();
    const forms = [domainFormRef, queryFormRef];

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
        >
            <div className="controls-node" style={{ position: 'fixed', left: 10, top: 10, zIndex: 4, backgroundColor: "#ffffff", textAlign: "left" }}>
                <button onClick={() => { setControlsHidden(!controlsHidden) }}>Show/Hide Forms</button>
                <div hidden={controlsHidden}>
                    <fieldset disabled={controlFormLock}>
                        <b>CURRENT GRAPH INFO:</b><br />
                        {getMessage("currentlyLoadedMessage")}<br />
                        <button onClick={clearFlow}>Clear Graph</button>
                    </fieldset>
                    <DomainLevelForm
                        formLock={controlFormLock}
                        setFormLock={updateControlFormLock}
                        setMessage={setMessage}
                        searchParams={searchParams}
                        setSearchParams={updateSearchParams}
                        clearFlow={clearFlow}
                        loadFlow={loadFlow}
                        ref={domainFormRef}
                    />
                    <QueryLevelForm
                        formLock={controlFormLock}
                        setFormLock={updateControlFormLock}
                        setMessage={setMessage}
                        searchParams={searchParams}
                        setSearchParams={updateSearchParams}
                        clearFlow={clearFlow}
                        loadFlow={loadFlow}
                        ref={queryFormRef}
                    />
                    { /*<OnDemandForm
                        formLock={controlFormLock}
                        setFormLock={updateControlFormLock}
                        setMessage={setMessage}
                        searchParams={searchParams}
                        setSearchParams={updateSearchParams}
                        clearFlow={clearFlow}
                        loadFlow={loadFlow}
                    />
                    <TestButtonForm
                        formLock={controlFormLock}
                        setFormLock={updateControlFormLock}
                        setMessage={setMessage}
                        searchParams={searchParams}
                        setSearchParams={updateSearchParams}
                        clearFlow={clearFlow}
                        loadFlow={loadFlow}
                    /> */}
                    {getColorHelpText()}
                    <div style={{ maxWidth: "300px", color: "red" }}>{getMessage("errorMessage")}</div>
                </div>
            </div>
            <Controls style={{position: "fixed"}}/>
            <MiniMap style={{position: "fixed"}}/>
            <Background variant="lines" gap={20} size={1} />
        </ReactFlow>
    );
};

export default Flow;