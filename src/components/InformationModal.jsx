function InformationModal({
  open,
  title,
  eyebrow,
  onClose,
  children,
  footer,
}) {
  if (!open) {
    return null;
  }

  return (
    <div
      className="informationModalOverlay"
      role="dialog"
      aria-modal="true"
      aria-labelledby="information-modal-title"
      onClick={onClose}
    >
      <div
        className="informationModal"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="lineAccent" aria-hidden="true" />

        <header className="informationModalHeader">
          <div className="informationModalHeading">
            {eyebrow ? (
              <span className="informationModalEyebrow">{eyebrow}</span>
            ) : null}
            <h2 id="information-modal-title" className="informationModalTitle">
              {title}
            </h2>
          </div>

          <div className="informationModalHeaderRail" aria-hidden="true" />

          <div className="informationModalHeaderDots" aria-hidden="true">
            <span />
            <span />
            <span />
          </div>

          <button
            type="button"
            className="informationModalClose"
            aria-label="Close information modal"
            onClick={onClose}
          >
            <span aria-hidden="true">×</span>
          </button>
        </header>

        <div className="informationModalBody">{children}</div>

        <footer className="informationModalFooter">{footer}</footer>
      </div>
    </div>
  );
}

export default InformationModal;
