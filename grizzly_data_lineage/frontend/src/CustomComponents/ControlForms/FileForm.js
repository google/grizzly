import ControlForm from "./ControlForm";

class FileForm extends ControlForm {
    constructor(props) {
        super(props);
        this.state = {
            file: null,
        }
    }

    handleSubmit = (event) => {
        event.preventDefault();
        if (!this.state.file) {
            this.props.setErrorMessage("Please choose a file.");
            return;
        }
        this.props.setErrorMessage("");
        const formData = new FormData();
        formData.append("file", this.state.file);

        this.props.clearFlow();
        const callback = (data) => { this.props.loadFlow(data.objects, data.connections); }
        this.makeRequest("/parse_sql_file", callback);
    }

    getFormContents = () => {
        return (
            <>
                <label><b>FROM FILE:</b></label><br />
                <input
                    type="file"
                    onChange={(event) => { this.setState({ file: event.target.files[0] }) }}
                /><br />
                <input type="submit" value="Submit" />
            </>
        );
    }
}

export default FileForm;