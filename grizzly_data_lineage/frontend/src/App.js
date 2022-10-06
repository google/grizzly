import { ReactFlowProvider } from 'react-flow-renderer';
import Flow from "./Flow";
import "./styles.css";

export default function App() {
  return (
    <div className="App">
      <ReactFlowProvider>
        <Flow />
      </ReactFlowProvider>
    </div>
  );
}
