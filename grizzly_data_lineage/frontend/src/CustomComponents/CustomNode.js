import React, { memo } from 'react';
import { Handle } from 'react-flow-renderer';

const CustomNode = memo(({ id, data, selected }) => {
    const sourceHandle = (
        <Handle
            type="source"
            position="right"
            isConnectable={false}
        />
    )
    const targetHandle = (
        <Handle
            type="target"
            position="left"
            isConnectable={false}
        />
    )
    return (
        <>
            <div style={{whiteSpace: "pre-wrap"}}><b>{data.label}</b></div>
            {data.hasOutboundConnection && sourceHandle}
            {data.hasInboundConnection && targetHandle}
        </>
    );
});

export default CustomNode;