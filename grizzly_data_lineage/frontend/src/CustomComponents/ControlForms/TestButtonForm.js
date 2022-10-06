import ControlForm from "./ControlForm";

class TestButtonForm extends ControlForm {
    constructor(props) {
        super(props);
        this.state = {
            requestURL: "/parse_test",
            requestInputs: [],
            immediateUpdateInputs: [],
            formLabel: "TEST",
        };
    }

    getFormContents = () => {
        return (
            <>
                <input type="submit" value="Parse Test Query" />
            </>
        );
    }
}

export default TestButtonForm;